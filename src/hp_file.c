#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_OR_DIE(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return -1;        \
  }                         \
}

#define CALL_OR_DIE_POINTER(call) \
{                                 \
    BF_ErrorCode code = call;     \
    if (code != BF_OK) {          \
        BF_PrintError(code);      \
        return NULL;              \
    }                             \
}


int SetUpNewBlock(HP_info *hp_info, Record record);

int HP_CreateFile(char *fileName){
    int fileDesc;
    void *data;
    HP_info *hp_info;
    BF_Block *firstBlock;

    /* Διαγραφή αρχείου για το heap implementation, που μπορεί να έχει απομείνει από προηγούμενη εκτέλεση, δημιουργεία και άνοιγμα νέου αρχείου hp*/
    remove(fileName);
    CALL_OR_DIE(BF_CreateFile(fileName));
    CALL_OR_DIE(BF_OpenFile(fileName, &fileDesc));

    /* Δημιουργία πρώτου μπλοκ και αποθήκευση μεταδεδομένων του heap σε αυτό*/
    BF_Block_Init(&firstBlock);

    CALL_OR_DIE(BF_AllocateBlock( fileDesc, firstBlock));
    hp_info = (HP_info *) BF_Block_GetData(firstBlock);

    BF_Block_SetDirty(firstBlock);
    CALL_OR_DIE(BF_UnpinBlock(firstBlock));

    BF_Block_Destroy(&firstBlock);
    
    /* Κλείσιμο αρχείου */
    CALL_OR_DIE(BF_CloseFile(fileDesc));
    return 0;
}

HP_info* HP_OpenFile(char *fileName){
    HP_info *fileInfo;
    BF_ErrorCode code;

    fileInfo = malloc(sizeof(HP_info*));
    if(fileInfo == NULL){
        return NULL;
    }
    /* Άνοιγμα του αρχείου και ενημέρωση του fileDescriptor*/
    CALL_OR_DIE_POINTER(BF_OpenFile(fileName, &fileInfo->FileDescriptor));
    
    return fileInfo;
}


int HP_CloseFile( HP_info* hp_info ){
    /* Κλείσιμο αρχείου*/
    CALL_OR_DIE(BF_CloseFile(hp_info->FileDescriptor));
    return 0;
}

int HP_InsertEntry(HP_info* hp_info, Record record){
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    Record *records;
    HP_block_info block_info;
    CALL_OR_DIE(BF_GetBlockCounter(hp_info->FileDescriptor, &numberOfBlocks));

    /* Αν το μόνο μπλοκ που υπάρχει είναι το πρώτομ, δημιούργησε καινούριο και αποθήκευσε την νέα εγγραφή*/
    if(numberOfBlocks == 1){
        return (SetUpNewBlock(hp_info, record));
    }

    BF_Block_Init(&block);
    /* Εύρεση τελευταίου μπλοκ στο αρχείο */
    CALL_OR_DIE(BF_GetBlock(hp_info->FileDescriptor, numberOfBlocks - 1, block));
    data = BF_Block_GetData(block);
    memcpy(&block_info, (data + BF_BLOCK_SIZE - sizeof(HP_block_info)), sizeof(HP_block_info));
    
    /* Αν το τελευταίο μπλοκ είναι γεμάτο φτιάξε νέο μπλοκ και αποθήκευσε την νέα εγγραφή*/
    if(block_info.RecordCount == RECORDS_PER_BLOCK){
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        
        return (SetUpNewBlock(hp_info, record));
    }
    
    /* Αποθήκευση νέας εγγραφής και ενημέρωση μεταδεδομένων του μπλοκ*/
    records = (Record*)data;
    records[block_info.RecordCount] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount++;
    memcpy( (data + BF_BLOCK_SIZE - sizeof(HP_block_info)), &block_info, sizeof(HP_block_info));

    BF_Block_Destroy(&block);
    return numberOfBlocks - 1;

}

int HP_GetAllEntries(HP_info* hp_info, int value){
    int recordCounter,
        counter, 
        blocksInFile, 
        blocksSearched, 
        fileFound;
    void *data;
    Record *records;
    BF_Block *block;
    HP_block_info block_info;
    
    /* Εύρεση του συνόλου των μπλοκ στο αρχείο */
    CALL_OR_DIE(BF_GetBlockCounter(hp_info->FileDescriptor, &blocksInFile));


    BF_Block_Init(&block);

    blocksSearched = 0;
    fileFound = 0;

    /* Ξεκινώντας από το δεύτερο μπλοκ του αρχείου, γίνεται σειριακή αναζήτηση στις εγγραφές του κάθε μπλοκ μέχρι να βρείς τη συγκεκριμένη εγγραφή.
    Μόλις βρεθεί, τυπώνεται η εγγραφή και επιστρέφεται ο αριθμός των blocks. Αν δε βρεθεί η εγγραφή απλά τυπώνεται σχετικό μήνυμα. */
    for(counter = 1; counter < blocksInFile; counter++){

        CALL_OR_DIE(BF_GetBlock(hp_info->FileDescriptor, counter, block));
        data = BF_Block_GetData(block);
        memcpy( &block_info, (data + BF_BLOCK_SIZE - sizeof(HP_block_info)), sizeof(HP_block_info));
        records = (Record*) data;

        for(recordCounter = 0; recordCounter < block_info.RecordCount; recordCounter++){
            if(records[recordCounter].id == value){
                printRecord(records[recordCounter]);
                CALL_OR_DIE(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
                return blocksSearched;
            }
        }
        CALL_OR_DIE(BF_UnpinBlock(block));
        blocksSearched++;
    }
    BF_Block_Destroy(&block);
    if(blocksSearched == blocksInFile){
        printf("Record with id: %d was not found", value);
    }
    return blocksSearched;
}

/* Βοηθητική συνάρτηση δημιουργίας νέου μπλοκ */
int SetUpNewBlock(HP_info *hp_info, Record record){
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    Record *blockData;
    HP_block_info block_info;

    BF_Block_Init(&block);

    CALL_OR_DIE(BF_AllocateBlock(hp_info->FileDescriptor, block));
    data = BF_Block_GetData(block);
    blockData = (Record *) data;
    blockData[0] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount = 1;
    memcpy( (data + BF_BLOCK_SIZE - sizeof(HP_block_info)), &block_info, sizeof(HP_block_info));

    BF_Block_Destroy(&block);

    CALL_OR_DIE(BF_GetBlockCounter(hp_info->FileDescriptor, &numberOfBlocks));
    return numberOfBlocks - 1;

}


