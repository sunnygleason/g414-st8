
grammar Query;

options {
  output=AST;
}

tokens {
  AND  = 'and' ;
  EQ   = 'eq' ;
  NE   = 'ne' ;
  LT   = 'lt' ;
  LE   = 'le' ;
  GT   = 'gt' ;
  GE   = 'ge' ;
  TRUE = 'true' ;
  FALSE = 'false' ;
  NULL = 'null' ;
}

@parser::header { package com.g414.st8.query; }
@lexer::header  { package com.g414.st8.query; }

@members {
@Override
public void reportError(RecognitionException e) {
    throw new RuntimeException(e);
}
}


/*------------------------------------------------------------------
 * PARSER RULES
 *------------------------------------------------------------------*/

term_list [List<QueryTerm> inList] :
        term[$inList] (AND term[$inList])* -> (term)+;

term [List<QueryTerm> inList] :
        (f=field o=op^ v=value)            { inList.add(new QueryTerm(o.value, f.value, v.value)); };

field returns [String value] :
        i=IDENT                            { $value = i.getText(); }
        ;

op returns [QueryOperator value] :
        EQ                                 { $value = QueryOperator.EQ; }
        | NE                               { $value = QueryOperator.NE; }
        | LT                               { $value = QueryOperator.LT; }
        | LE                               { $value = QueryOperator.LE; }
        | GT                               { $value = QueryOperator.GT; }
        | GE                               { $value = QueryOperator.GE; }
        ;

value returns [QueryValue value] :
        s=STRING_LITERAL                   { $value = new QueryValue(ValueType.STRING, s.getText()); }
        | i=INTEGER                        { $value = new QueryValue(ValueType.INTEGER, i.getText()); }
        | d=DECIMAL                        { $value = new QueryValue(ValueType.DECIMAL, d.getText()); }
        | TRUE                             { $value = new QueryValue(ValueType.BOOLEAN, "true"); }
        | FALSE                            { $value = new QueryValue(ValueType.BOOLEAN, "false"); }
        | NULL                             { $value = new QueryValue(ValueType.NULL, "null"); }
        ;


/*------------------------------------------------------------------
 * LEXER RULES
 *------------------------------------------------------------------*/

IDENT
  : ('_'|'a'..'z'|'A'..'Z') ('_'|'a'..'z'|'A'..'Z'|'0'..'9')*
  ;

WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ 	{ $channel = HIDDEN; } ;

fragment DIGIT : '0'..'9' ;
INTEGER : '-'? (DIGIT)+ ;
DECIMAL : '-'? (DIGIT)+ ('.' (DIGIT)+ )? ;

STRING_LITERAL
  : '"'!
    ( '"' '"'!
    | ~('"'|'\n'|'\r')
    )*
    ( '"'!
    | // nothing -- write error message
    )
   ;
 