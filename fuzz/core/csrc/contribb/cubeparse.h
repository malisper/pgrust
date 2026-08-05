/*
 * GENERATED FILE — part of the vendored PostgreSQL 18.3 oracle (contribb_diff).
 * Generated from the VERBATIM vendored grammar/scanner (never hand-edited):
 *   input: ~/dev/pgrust-fabled/vendor/postgres-src/contrib/cube/cubeparse.y
 *   input sha256: ac045108f62e7ace0580b5dde119143239c01d774db3e2f4aac1d10fa9216269
 *   vendor tree: Stamp-18.3, upstream sha 62d6c7d3df6287f1bd83199c1a746e50d31571a0
 *   generator: /usr/bin/bison (GNU Bison) 2.3, invoked: bison -d -o cubeparse.c cubeparse.y
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
     CUBEFLOAT = 258,
     O_PAREN = 259,
     C_PAREN = 260,
     O_BRACKET = 261,
     C_BRACKET = 262,
     COMMA = 263
   };
#endif
/* Tokens.  */
#define CUBEFLOAT 258
#define O_PAREN 259
#define C_PAREN 260
#define O_BRACKET 261
#define C_BRACKET 262
#define COMMA 263




#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef int YYSTYPE;
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
# define YYSTYPE_IS_TRIVIAL 1
#endif



