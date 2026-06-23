//! libxml2 provider for `backend-utils-adt-xml-libxml-seams`.
//!
//! This is the `--with-libxml` provider: it binds the *same* system libxml2
//! PostgreSQL binds and installs the libxml2 FFI seams declared in
//! `backend-utils-adt-xml-libxml-seams` (the ported pure-logic owner
//! `backend-utils-adt-xml` already implements the XML-declaration parsing, the
//! SQL-identifier mapping, the XSD value formatting, etc.; only the libxml2
//! calls — parse, serialize, XPath, the `xmlTextWriter` element/binary builders
//! — cross the seam, and they land here).
//!
//! Feature-gated as `with-libxml`. With the feature OFF (`#ifdef USE_LIBXML`
//! false), [`init_seams`] installs only `have_libxml` (returning `false`), so
//! the in-crate bodies raise `NO_XML_SUPPORT()` exactly as a `--without-libxml`
//! server does, and never reach the parse/serialize seams.
//!
//! This crate binds *only* the libxml2 half of the xml.c outward seams. The
//! cross-subsystem seams (SPI, syscache/catalog, encoding conversions, utils/mb)
//! are not libxml2 and are installed by their own real owners.

#![allow(non_snake_case)]
#![allow(non_camel_case_types)]

#[cfg(feature = "with-libxml")]
mod provider;

/// Install the libxml2 FFI seams (binds the system libxml2). Call once at
/// startup. With the `with-libxml` feature off this installs only
/// `have_libxml` (returning `false`) — faithful `#ifdef USE_LIBXML` off.
pub fn init_seams() {
    #[cfg(feature = "with-libxml")]
    provider::install();

    #[cfg(not(feature = "with-libxml"))]
    xml_libxml_seams::have_libxml::set(|| false);
}
