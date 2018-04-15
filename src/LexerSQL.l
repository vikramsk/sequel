/***********************************************************************
 * SECTION 1
 ***********************************************************************/
/* The code in %{ %} is included as it is in lex.yysql.c file
 * it has C global variables, prototypes, and comments 
 */


%{

#include <string.h> // For strdup()
#include <stdlib.h> // For malloc()
#include "ParseTree.h"
#include "yysql.tab.h"

int lineno = 1;
void yysqlerror(char*s);

%}


/******************************************************************************
 * SECTION 2
 ******************************************************************************/
/* This is the DEFINITION section which contains substitutions, code, and
 * start stats; will be copied into lex.yysql.c
 */

/******************************************************************************
 * SECTION 3
 ******************************************************************************/
/* This is the RULES section which defines how to "scan" and what action
 * to take for each token
 */

%%

"SELECT"		return(SELECT);

"FROM"			return(FROM);

"WHERE"			return(WHERE);

"SUM"			return(SUM);

"AND"			return(AND);

"GROUP"			return(GROUP);

"DISTINCT"		return(DISTINCT);

"BY"			return(BY);

"OR"			return(OR);

"AS"			return(AS);

"("			return('(');

"<"                     return('<');

">"                     return('>');

"="                     return('=');

")"    	        	return(')');

"+"    	        	return('+');

"-"    	        	return('-');

"/"    	        	return('/');

"*"    	        	return('*');

","    	        	return(',');

-?[0-9]+ 	       {yysqllval.actualChars = strdup(yysqltext);
  			return(Int); 
		        }

-?[0-9]+\.[0-9]*       {yysqllval.actualChars = strdup(yysqltext); 
  			return(Float);
			}

[A-Za-z][A-Za-z0-9_-]* {yysqllval.actualChars = strdup(yysqltext);
  			return(Name);
			}     

[A-Za-z][A-Za-z0-9_-]*\.[A-Za-z][A-Za-z0-9_-]* {yysqllval.actualChars = strdup(yysqltext);
  			return(Name);
			}     

\'[^'\n]*\'            {/* take care of ' in a string */
                        if (yysqltext[yysqlleng - 2] == '\\') {
                                yymore();
                        } else {
                                yysqllval.actualChars = strdup(yysqltext + 1);
                                yysqllval.actualChars[strlen(yysqllval.actualChars) - 1] = 0;
                                return(String);
                        }
                        }

\n                	lineno++;

[ \t]             	;

.                 	yysqlerror("LEX_ERROR: invalid character");

%%

void yysqlerror(char *s) {
  printf("%d: %s at %s\n", lineno, s, yysqltext);
}

int yysqlwrap(void){
  return 1;
}

