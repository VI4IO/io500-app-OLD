#ifndef _IO500_UTILS_H
#define _IO500_UTILS_H

#include <stdio.h>

#include "io500-types.h"

extern int io500_rank;

FILE * io500_prepare_out(char * suffix, int testID, io500_options_t * options);

void io500_replace_str(char * str);
char ** io500_str_to_arr_prep_exec(char * str, int * out_count);
void io500_error(char * const str);

#endif
