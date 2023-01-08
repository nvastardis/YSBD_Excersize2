#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"

#define RECORDS_NUM 200 // you can change it if you want
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

  if(HT_CreateFile(FILE_NAME,10)){
    return -1;
  }

  HT_info* info = HT_OpenFile(FILE_NAME);
  if(info == NULL){
    return -1;
  }

  Record record;
  srand(12569874);
  int r;
  printf("Insert Entries\n");
  for (int id = 0; id < RECORDS_NUM; ++id) {
    record = randomRecord();
    if(HT_InsertEntry(info, record) != 0){
      printf("Error inserting record %d", id);
      return -1;
    }
  }

  printf("RUN PrintAllEntries\n");
  int id = rand() % RECORDS_NUM;
  if(HT_GetAllEntries(info, &id) == -1){
    return -1;
  }

  HT_CloseFile(info);
  BF_Close();
}
