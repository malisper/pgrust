//! `backend-nodes-macros` — `#[derive(PgNode)]` derive macro for PostgreSQL
//! node support (`copyfuncs.c` / `equalfuncs.c`), idiomatic owned-tree port
//! re-homed onto `mcx`.
//!
//! # Why
//!
//! PostgreSQL does not hand-write `copyfuncs.c` and `equalfuncs.c`. It
//! *generates* them from the field-level `pg_node_attr(...)` annotations on
//! each node struct, via `src/backend/nodes/gen_node_support.pl`. There are
//! ~397 node structs; hand-porting copy/equal for each is both enormous and a
//! correctness hazard (every field must be handled, and the C generator
//! already encodes the per-field rules).
//!
//! This crate does the same code-generation in Rust as a `#[derive(PgNode)]`
//! proc-macro reading field/struct attributes that mirror `pg_node_attr`. It
//! covers **copy + equal**. The OUT/READ (`outfuncs.c`/`readfuncs.c`) and
//! JUMBLE (`queryjumblefuncs.c`) stages are deferred — this crate emits neither.
//!
//! # The two generated methods
//!
//! `#[derive(PgNode)]` on a struct generates `impl PgNodeCopy` and
//! `impl PgNodeEqual` for it (unless a struct attribute suppresses one or both):
//!
//! * `copy_node_in(&self, __dst: Mcx) -> PgResult<Self::Bound<'dst>>` — the
//!   fallible owned-tree analogue of `_copyFoo` in `copyfuncs.c`. C's
//!   `copyObject` deep-copies into `CurrentMemoryContext`; here the destination
//!   context is threaded explicitly, every field is deep-copied via
//!   [`PgNodeCopy::copy_node_in`] into `__dst`, and a charged allocation that
//!   hits the context limit surfaces as `Err` (the C `ereport(ERROR)` on OOM).
//!   The associated `type Bound<'dst>` is the node re-parameterized to live in
//!   `__dst`: a node `Foo<'mcx>` has `Bound<'dst> = Foo<'dst>`, a lifetime-free
//!   struct has `Bound<'dst> = Self`.
//! * `equal_node(&self, other: &Self) -> bool` — the analogue of `_equalFoo`
//!   in `equalfuncs.c`. Every field is compared via [`PgNodeEqual::equal_node`].
//!   Infallible and lifetime-agnostic.
//!
//! # Uniform-dispatch design (the key simplification)
//!
//! The C macros (`COPY_SCALAR_FIELD`, `COPY_NODE_FIELD`, `COPY_STRING_FIELD`,
//! `COPY_LOCATION_FIELD`, …) exist because C has no traits: the generator must
//! pick a different macro per field *type*. In Rust we collapse all of those
//! into a single uniform call site: every field, scalar or node, is copied with
//! `field.copy_node_in(__dst)?` and compared with `a.equal_node(&b)`.
//!
//! # Field attributes (mirroring `pg_node_attr`)
//!
//! Written as `#[pg_node(...)]` on a field. See `gen_node_support.pl` for the C
//! semantics:
//!
//! * `#[pg_node(location)]` — a parse-location field (`ParseLoc` in C). Copied
//!   verbatim (`COPY_LOCATION_FIELD`); ignored in equal
//!   (`COMPARE_LOCATION_FIELD` is a no-op). Implies `equal_ignore`.
//! * `#[pg_node(copy_ignore)]` — do not copy this field; reset it to
//!   `Default::default()` in the copy (C's `palloc0` zero).
//! * `#[pg_node(equal_ignore)]` — skip this field in `equal_node`.
//! * `#[pg_node(equal_ignore_if_zero)]` — compare, but treat the field as equal
//!   if *either* side is zero (its `Default`).
//! * `#[pg_node(copy_as_scalar)]` — force a flat scalar copy (`Clone::clone`),
//!   no recursion (`COPY_SCALAR_FIELD` override).
//! * `#[pg_node(equal_as_scalar)]` — force a flat scalar compare (`==`), no
//!   recursion (`COMPARE_SCALAR_FIELD` override).
//! * `#[pg_node(array_size(count_field))]` — the field is a `PgVec<T>` whose live
//!   length is held in the sibling scalar field `count_field`. Copy and compare
//!   only the first `count_field` elements (`COPY_POINTER_FIELD` /
//!   `COMPARE_POINTER_FIELD`). `count_field` must precede this field.
//! * `#[pg_node(copy_as(other_field))]` — on copy, set this field from the
//!   sibling field `other_field` instead of from itself (C's
//!   `newnode->f = other_field`).
//!
//! # Struct attributes
//!
//! Written as `#[pg_node(...)]` on the struct:
//!
//! * `#[pg_node(no_copy)]` — do not generate `impl PgNodeCopy`.
//! * `#[pg_node(no_equal)]` — do not generate `impl PgNodeEqual`.
//! * `#[pg_node(custom_copy_equal)]` — generate neither copy nor equal; a
//!   hand-written `PgNodeCopy`/`PgNodeEqual` impl is expected.
//! * `#[pg_node(nodetag_only)]` — the type only needs a node tag, no support
//!   functions; generate neither copy nor equal.
//!
//! Any unrecognised attribute (field or struct) is a hard compile error, the
//! same spirit as `gen_node_support.pl` dying on an unknown attribute. The
//! OUT/READ/JUMBLE-stage attributes are deferred and are NOT accepted by this
//! crate.

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, GenericParam, Ident, Lifetime};

/// Per-field flags decoded from `#[pg_node(...)]` attributes.
#[derive(Default, Clone)]
struct FieldAttrs {
    location: bool,
    copy_ignore: bool,
    equal_ignore: bool,
    equal_ignore_if_zero: bool,
    copy_as_scalar: bool,
    equal_as_scalar: bool,
    /// `array_size(count_field)` — sibling field holding the live length.
    array_size: Option<Ident>,
    /// `copy_as(other_field)` — sibling field to copy from instead of self.
    copy_as: Option<Ident>,
}

impl FieldAttrs {
    /// Decode every `#[pg_node(...)]` attribute on a field. Unknown keys are a
    /// hard compile error so a typo in a real-node annotation can never be
    /// silently dropped (the same spirit as `gen_node_support.pl` dying on an
    /// unrecognised attribute).
    fn from_field(field: &syn::Field) -> syn::Result<Self> {
        let mut attrs = FieldAttrs::default();
        for attr in &field.attrs {
            if !attr.path().is_ident("pg_node") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("location") {
                    attrs.location = true;
                } else if meta.path.is_ident("copy_ignore") {
                    attrs.copy_ignore = true;
                } else if meta.path.is_ident("equal_ignore") {
                    attrs.equal_ignore = true;
                } else if meta.path.is_ident("equal_ignore_if_zero") {
                    attrs.equal_ignore_if_zero = true;
                } else if meta.path.is_ident("copy_as_scalar") {
                    attrs.copy_as_scalar = true;
                } else if meta.path.is_ident("equal_as_scalar") {
                    attrs.equal_as_scalar = true;
                } else if meta.path.is_ident("array_size") {
                    // array_size(count_field): parenthesised sibling field name.
                    let content;
                    syn::parenthesized!(content in meta.input);
                    attrs.array_size = Some(content.parse::<Ident>()?);
                } else if meta.path.is_ident("copy_as") {
                    // copy_as(other_field): parenthesised sibling field name.
                    let content;
                    syn::parenthesized!(content in meta.input);
                    attrs.copy_as = Some(content.parse::<Ident>()?);
                } else {
                    return Err(meta.error("unknown pg_node field attribute"));
                }
                Ok(())
            })?;
        }
        // `location` is `equal_ignore` for comparison purposes (C's
        // COMPARE_LOCATION_FIELD is a no-op).
        if attrs.location {
            attrs.equal_ignore = true;
        }
        Ok(attrs)
    }
}

/// Struct-level flags decoded from `#[pg_node(...)]` on the struct itself.
#[derive(Default, Clone, Copy)]
struct StructAttrs {
    no_copy: bool,
    no_equal: bool,
}

impl StructAttrs {
    fn from_derive_input(input: &DeriveInput) -> syn::Result<Self> {
        let mut attrs = StructAttrs::default();
        for attr in &input.attrs {
            if !attr.path().is_ident("pg_node") {
                continue;
            }
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("no_copy") {
                    attrs.no_copy = true;
                } else if meta.path.is_ident("no_equal") {
                    attrs.no_equal = true;
                } else if meta.path.is_ident("custom_copy_equal") {
                    // Hand-written copy AND equal expected: emit neither.
                    attrs.no_copy = true;
                    attrs.no_equal = true;
                } else if meta.path.is_ident("nodetag_only") {
                    // Only a node tag, no support functions: emit none.
                    attrs.no_copy = true;
                    attrs.no_equal = true;
                } else {
                    return Err(meta.error("unknown pg_node struct attribute"));
                }
                Ok(())
            })?;
        }
        Ok(attrs)
    }
}

/// `#[derive(PgNode)]` — generate `copy_node_in` / `equal_node` for a node
/// struct.
///
/// See the crate docs for the attribute model and the uniform-dispatch design.
#[proc_macro_derive(PgNode, attributes(pg_node))]
pub fn derive_pg_node(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand(&input) {
        Ok(ts) => ts.into(),
        Err(e) => e.to_compile_error().into(),
    }
}

/// True if `ty` is a (possibly path-qualified) type whose final segment is
/// `name` — e.g. `Plan`, `crate::Plan`, `nodeindexscan::Plan` all match
/// `type_path_tail_is(ty, "Plan")`.
fn type_path_tail_is(ty: &syn::Type, name: &str) -> bool {
    if let syn::Type::Path(tp) = ty {
        if let Some(seg) = tp.path.segments.last() {
            return seg.ident == name;
        }
    }
    false
}

fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let name = &input.ident;
    let struct_attrs = StructAttrs::from_derive_input(input)?;

    // Node structs are `'mcx`-parameterized (their charged child links carry
    // `PgVec<'mcx,_>`/`PgBox<'mcx,_>`/`PgString<'mcx>`), while a few leaf node
    // structs are lifetime-free. The copy targets a DIFFERENT context `'dst`, so
    // the associated `Bound<'dst>` must re-parameterize the node's lifetime:
    //   * a struct with a single lifetime param `Foo<'mcx>` -> `Bound<'dst> =
    //     Foo<'dst>` (re-home every child link into the destination context);
    //   * a lifetime-free struct -> `Bound<'dst> = Self` (nothing to re-home).
    // We collect the struct's lifetime params (any const/type generics would
    // need threading too, but the node model uses at most one lifetime and no
    // type/const generics, so we reject those loudly rather than mis-handle).
    let lifetimes: Vec<&Lifetime> = input
        .generics
        .params
        .iter()
        .filter_map(|p| match p {
            GenericParam::Lifetime(l) => Some(&l.lifetime),
            _ => None,
        })
        .collect();
    if input
        .generics
        .params
        .iter()
        .any(|p| !matches!(p, GenericParam::Lifetime(_)))
    {
        return Err(syn::Error::new_spanned(
            name,
            "#[derive(PgNode)] supports only lifetime generics on a node struct \
             (type/const generics are not part of the node model)",
        ));
    }
    if lifetimes.len() > 1 {
        return Err(syn::Error::new_spanned(
            name,
            "#[derive(PgNode)] supports at most one lifetime parameter on a node struct",
        ));
    }
    // The `impl` generics for the trait impl header (`impl<'mcx> ... for
    // Foo<'mcx>`), and the `Self`-as-written type (`Foo<'mcx>` or `Foo`).
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    // The `Bound<'dst>` associated type body: `Foo<'dst>` (rebind the single
    // lifetime to `'dst`) or `Self` (lifetime-free).
    let bound_ty = if lifetimes.is_empty() {
        quote! { Self }
    } else {
        quote! { #name<'dst> }
    };
    // The struct-literal constructor for the copied value. In *type* position
    // `Foo<'dst>` is fine (that's `bound_ty`), but a struct *expression* must use
    // turbofish for the generic (`Foo::<'dst> { .. }`) — `Foo<'dst> { .. }`
    // mis-parses `<` as a comparison. A lifetime-free struct is just `Foo`.
    let bound_ctor = if lifetimes.is_empty() {
        quote! { #name }
    } else {
        quote! { #name::<'dst> }
    };

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(named) => &named.named,
            _ => {
                return Err(syn::Error::new_spanned(
                    name,
                    "#[derive(PgNode)] requires named fields",
                ))
            }
        },
        _ => {
            return Err(syn::Error::new_spanned(
                name,
                "#[derive(PgNode)] can only be applied to structs",
            ))
        }
    };

    // Per-field copy initialisers and equal comparisons.
    let mut copy_inits = Vec::new();
    let mut equal_checks = Vec::new();
    // Track fields already seen, to validate that an array_size/copy_as target
    // precedes the field referencing it (mirrors gen_node_support.pl's
    // "array size field ... must precede" die check).
    let mut seen_fields: Vec<Ident> = Vec::new();

    for field in fields.iter() {
        let ident: &Ident = field.ident.as_ref().expect("named field");
        let attrs = FieldAttrs::from_field(field)?;

        // Validate cross-field references precede this field.
        if let Some(count) = &attrs.array_size {
            if !seen_fields.iter().any(|f| f == count) {
                return Err(syn::Error::new_spanned(
                    field,
                    format!(
                        "array_size field `{count}` must be declared before field `{ident}`"
                    ),
                ));
            }
        }
        if let Some(src) = &attrs.copy_as {
            if !seen_fields.iter().any(|f| f == src) {
                return Err(syn::Error::new_spanned(
                    field,
                    format!("copy_as field `{src}` must be declared before field `{ident}`"),
                ));
            }
        }

        // ---- copy_node_in ----
        // Mirrors copyfuncs.c: emit a per-field initialiser. The uniform
        // `.copy_node_in(__dst)?` call dispatches to scalar-clone or
        // node-recursion via the PgNodeCopy trait, re-homing the allocation onto
        // the target context `__dst`; the override attributes emit direct code.
        if !struct_attrs.no_copy {
            if let Some(src) = &attrs.copy_as {
                // copy_as(other_field): newnode->f = other_field. The C code
                // assigns the pointer/value verbatim; for an owned tree we clone
                // the sibling's value so `self` stays valid. (copy_as targets a
                // lifetime-free scalar sibling, so `Clone` is the faithful copy.)
                copy_inits.push(quote! {
                    #ident: ::core::clone::Clone::clone(&self.#src)
                });
            } else if attrs.copy_ignore {
                // C emits no COPY_* line, leaving the palloc0 zero. Owned-tree
                // analogue: Default::default().
                copy_inits.push(quote! {
                    #ident: ::core::default::Default::default()
                });
            } else if attrs.copy_as_scalar {
                // COPY_SCALAR_FIELD override: flat clone, NO recursion (no
                // re-home — a scalar lives wherever its owning node does).
                copy_inits.push(quote! {
                    #ident: ::core::clone::Clone::clone(&self.#ident)
                });
            } else if let Some(count) = &attrs.array_size {
                // COPY_POINTER_FIELD: copy exactly `count` elements, recursing
                // into each into `__dst` (works for scalar or node element types
                // via the uniform copy_node_in dispatch).
                copy_inits.push(quote! {
                    #ident: {
                        let __n = (self.#count) as usize;
                        let mut __out = ::backend_nodes_node_support::mcx_vec_with_capacity_in(
                            __dst, __n,
                        )?;
                        for __e in self.#ident.iter().take(__n) {
                            __out.push(
                                ::backend_nodes_node_support::PgNodeCopy::copy_node_in(__e, __dst)?
                            );
                        }
                        __out
                    }
                });
            } else {
                // Covers COPY_SCALAR_FIELD / COPY_STRING_FIELD / COPY_NODE_FIELD
                // / COPY_LOCATION_FIELD (location copied verbatim, like C).
                copy_inits.push(quote! {
                    #ident: ::backend_nodes_node_support::PgNodeCopy::copy_node_in(
                        &self.#ident, __dst,
                    )?
                });
            }
        }

        // ---- equal_node ----
        // Mirrors equalfuncs.c: emit a per-field comparison, except for
        // equal_ignore / location (C emits no COMPARE_* line for those).
        if !struct_attrs.no_equal && !attrs.equal_ignore {
            if attrs.equal_ignore_if_zero {
                // COMPARE: unequal only if both sides differ AND both nonzero.
                // Mirrors: if (a->f != b->f && a->f != 0 && b->f != 0) return false;
                equal_checks.push(quote! {
                    {
                        let __a = &self.#ident;
                        let __b = &other.#ident;
                        let __zero: _ = ::core::default::Default::default();
                        if __a != __b && *__a != __zero && *__b != __zero {
                            return false;
                        }
                    }
                });
            } else if attrs.equal_as_scalar {
                // COMPARE_SCALAR_FIELD override: flat `==`, NO recursion.
                equal_checks.push(quote! {
                    if self.#ident != other.#ident {
                        return false;
                    }
                });
            } else if let Some(count) = &attrs.array_size {
                // COMPARE_POINTER_FIELD: compare exactly `count` elements
                // element-wise. (The count field — compared earlier — enforces
                // the live counts agree; the slice here guards differing prefixes.)
                equal_checks.push(quote! {
                    {
                        let __na = (self.#count) as usize;
                        let __nb = (other.#count) as usize;
                        if __na != __nb {
                            return false;
                        }
                        for __i in 0..__na {
                            if !::backend_nodes_node_support::PgNodeEqual::equal_node(
                                &self.#ident[__i], &other.#ident[__i],
                            ) {
                                return false;
                            }
                        }
                    }
                });
            } else {
                equal_checks.push(quote! {
                    if !::backend_nodes_node_support::PgNodeEqual::equal_node(
                        &self.#ident, &other.#ident,
                    ) {
                        return false;
                    }
                });
            }
        }

        seen_fields.push(ident.clone());
    }

    // Assemble only the impls not suppressed by struct attributes. For
    // custom_copy_equal / nodetag_only / no_copy / no_equal we must NOT emit a
    // conflicting impl (a hand-written one is expected, or none is wanted).
    let copy_impl = if struct_attrs.no_copy {
        quote! {}
    } else {
        quote! {
            impl #impl_generics ::backend_nodes_node_support::PgNodeCopy for #name #ty_generics
                #where_clause
            {
                type Bound<'dst> = #bound_ty;
                fn copy_node_in<'dst>(
                    &self,
                    __dst: ::backend_nodes_node_support::Mcx<'dst>,
                ) -> ::backend_nodes_node_support::PgResult<Self::Bound<'dst>> {
                    ::core::result::Result::Ok(#bound_ctor {
                        #(#copy_inits,)*
                    })
                }
            }
        }
    };

    let equal_impl = if struct_attrs.no_equal {
        quote! {}
    } else {
        quote! {
            impl #impl_generics ::backend_nodes_node_support::PgNodeEqual for #name #ty_generics
                #where_clause
            {
                fn equal_node(&self, other: &Self) -> bool {
                    #(#equal_checks)*
                    true
                }
            }
        }
    };

    // ---- plan_base() inherent accessor (PG's `((Plan *) node)`) ----
    // Every plan node embeds a `Plan` base (directly as its first field, or via
    // an embedded `Scan`/`Join`/`Sort` whose own first field is `plan`). Emit a
    // uniform accessor so the opaque `Node` can read the common plan header of
    // ANY plan node without knowing its concrete type — the idiomatic analogue of
    // C's `((Plan *) node)` cast. Emitted ONLY when the first field is the
    // embedded base. The return path `crate::nodeindexscan::Plan` resolves in the
    // owning `types-nodes` crate.
    let plan_base_impl = {
        let base = fields.iter().next().and_then(|f| {
            let fid = f.ident.as_ref()?;
            if type_path_tail_is(&f.ty, "Plan") || type_path_tail_is(&f.ty, "PlanNode") {
                Some((quote! { &self.#fid }, quote! { &mut self.#fid }))
            } else if type_path_tail_is(&f.ty, "Scan")
                || type_path_tail_is(&f.ty, "Join")
                || type_path_tail_is(&f.ty, "Sort")
            {
                // The embedded base is one level deeper: Scan/Join/Sort each hold
                // their `Plan` as their own first field `plan`.
                Some((quote! { &self.#fid.plan }, quote! { &mut self.#fid.plan }))
            } else {
                None
            }
        });
        match base {
            Some((get, get_mut)) => {
                // Borrow the embedded `Plan<'_>` (its lifetime is the struct's own
                // lifetime). The accessor is lifetime-elided over `&self`.
                let plan_lt = lifetimes.first();
                let plan_ty = match plan_lt {
                    Some(lt) => quote! { crate::nodeindexscan::Plan<#lt> },
                    None => quote! { crate::nodeindexscan::Plan<'static> },
                };
                quote! {
                    impl #impl_generics #name #ty_generics #where_clause {
                        /// `((Plan *) node)` — borrow the embedded `Plan` base
                        /// header (the common plan fields). Generated by
                        /// `#[derive(PgNode)]`.
                        #[inline]
                        pub fn plan_base(&self) -> & #plan_ty { #get }
                        /// Mutable `((Plan *) node)`.
                        #[inline]
                        pub fn plan_base_mut(&mut self) -> &mut #plan_ty { #get_mut }
                    }
                }
            }
            None => quote! {},
        }
    };

    Ok(quote! {
        #copy_impl
        #equal_impl
        #plan_base_impl
    })
}
