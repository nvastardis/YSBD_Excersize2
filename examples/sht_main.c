#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "bf.h"
#include "ht_table.h"
#include "sht_table.h"

#define RECORDS_NUM 1400 // you can change it if you want
#define FILE_NAME "data.db"
#define INDEX_NAME "index.db"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }


int main() {
    srand(12569874);
    BF_Init(LRU);
    // Αρχικοποιήσεις
    HT_CreateFile(FILE_NAME,10);
    if(SHT_CreateSecondaryIndex(INDEX_NAME,10,FILE_NAME) == -1){
        return -1;
    }
    HT_info* info = HT_OpenFile(FILE_NAME);
    SHT_info* index_info = SHT_OpenSecondaryIndex(INDEX_NAME);
    if(index_info == NULL){
        return -1;
    }
    // Θα ψάξουμε στην συνέχεια το όνομα searchName
    Record record=randomRecord();
    char searchName[15];
    strcpy(searchName, record.name);

    // Κάνουμε εισαγωγή τυχαίων εγγραφών τόσο στο αρχείο κατακερματισμού τις οποίες προσθέτουμε και στο δευτερεύον ευρετήριο
    printf("Insert Entries\n");

    for (int id = 0; id < RECORDS_NUM; ++id) {
        int block_id;
        record = randomRecord();
        if( (block_id = HT_InsertEntry(info, record)) == -1){
            printf("Error Inserting Entry on HT %d", block_id);
        }
        if(SHT_SecondaryInsertEntry(index_info, record, block_id) == -1){
            printf("Error Inserting Entry on SHT %d", block_id);
            return -1;
        }
    }
    // Τυπώνουμε όλες τις εγγραφές με όνομα searchName
    printf("RUN PrintAllEntries for name %s\n", searchName);
    if(SHT_SecondaryGetAllEntries(info,index_info, searchName) == -1){
      return -1;
    }

    HT_info *ht_info = malloc(sizeof(HT_info));
    ht_info->FileDescriptor = info->FileDescriptor;
    ht_info->HashtableMapping = info->HashtableMapping;
    
    // Κλείνουμε το αρχείο κατακερματισμού και το δευτερεύον ευρετήριο
    SHT_CloseSecondaryIndex(index_info);
    HT_CloseFile(ht_info);
    SHT_HashStatistics(INDEX_NAME);
    HT_HashStatistics(FILE_NAME);
    BF_Close();
}
