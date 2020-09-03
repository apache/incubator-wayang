grammar MathEx;

options {
  language = Java;
}

expression
  : '(' expression ')' #parensExpression
  | operator=('-' | '+') expression #unaryOperation
  | operand0=expression operator='^' operand1=expression #binaryOperation
  | operand0=expression operator=('*' | '%' | '/') operand1=expression #binaryOperation
  | operand0=expression operator=('+' | '-') operand1=expression #binaryOperation
  | variableName=IDENTIFIER #variable
  | value=NUMBER #constant
  | name=IDENTIFIER '(' ( expression (',' expression )* )? ')' #function
  ;

fragment CHAR : [a-zA-Z] ;
fragment DIGIT : [0-9] ;
fragment INT : [1-9] DIGIT* | DIGIT ;
fragment EXP :   [Ee] [+\-]? INT ;

NUMBER
  :   '-'? INT? '.' [0-9]+ EXP? // 1.35, 1.35E-9, 0.3, -4.5
  |   '-'? INT EXP             // 1e10 -3e4
  |   '-'? INT                 // -3, 45
  ;

WS  :   [ \t\n\r]+ -> skip ;

PREC0_OP : [+\-] ;
PREC1_OP : [*%/] ;
PREC2_OP : '^' ;

IDENTIFIER
  : IDENTIFIER_START ( IDENTIFIER_MIDDLE* IDENTIFIER_END )?
  ;

fragment IDENTIFIER_START
  : '_'
  | [a-zA-Z]
  ;

fragment IDENTIFIER_MIDDLE
  : '_' | '.'
  | CHAR
  | DIGIT
  ;


fragment IDENTIFIER_END
  : '_'
  | CHAR
  | DIGIT
  ;
