/*
 * GENERATED FILE — part of the vendored PostgreSQL 18.3 oracle (contribb_diff).
 * Generated from the VERBATIM vendored grammar/scanner (never hand-edited):
 *   input: ~/dev/pgrust-fabled/vendor/postgres-src/contrib/seg/segparse.y
 *   input sha256: 5ecf14d196920cb2424d2db2336a247da2280f38aad8d0e7f9a3a1f5a438c16f
 *   vendor tree: Stamp-18.3, upstream sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *   generator: /usr/bin/bison (GNU Bison) 2.3, invoked: bison -d -o segparse.c segparse.y
 * Generated-from-verbatim-grammar counts as vendored (lane p1-mb-contribb
 * charter); regenerate with scratchpad/assemble_contribb.sh.
 */
/* A Bison parser, made by GNU Bison 2.3.  */

/* Skeleton interface for Bison's Yacc-like parsers in C

   Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2, or (at your option)
   any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor,
   Boston, MA 02110-1301, USA.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     SEGFLOAT = 258,
     RANGE = 259,
     PLUMIN = 260,
     EXTENSION = 261
   };
#endif
/* Tokens.  */
#define SEGFLOAT 258
#define RANGE 259
#define PLUMIN 260
#define EXTENSION 261




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
#line 41 "/Users/malisper/dev/pgrust-fabled/vendor/postgres-src/contrib/seg/segparse.y"
{
	struct BND
	{
		float		val;
		char		ext;
		char		sigd;
	} bnd;
	char	   *text;
}
/* Line 1529 of yacc.c.  */
#line 71 "segparse.h"
	YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



