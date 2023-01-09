#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "hp_file.h"

#define RECORDS_NUM 1000 // you can change it if you want
#define FILE_NAME "data.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int main() {
  BF_Init(LRU);

  if(HP_CreateFile(FILE_NAME)){
    printf("Error Creating File");
    return -1;
  }

  HP_info* info = HP_OpenFile(FILE_NAME);
  if(info == NULL){
    printf("Error opening file");
    return -1;
  }

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    if(HP_InsertEntry(info, record) == -1){
      printf("Error inserting record %d", id);
      return -1;
    }
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  printf("\nSearching for: %d\n",id);
  if(HP_GetAllEntries(info, id) == -1){
    printf("Error finding record with id: %d", id);
  }

  HP_CloseFile(info);
  BF_Close();
}
