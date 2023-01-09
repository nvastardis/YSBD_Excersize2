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

int SetUpNewBlock(HP_info *hp_info, Record record);

int HP_CreateFile(char *fileName){
    int fileDesc;
    void *data;
    HP_info *hp_info;
    BF_Block *firstBlock;

    remove(fileName);
    CALL_OR_DIE(BF_CreateFile(fileName));
    CALL_OR_DIE(BF_OpenFile(fileName, &fileDesc));

    BF_Block_Init(&firstBlock);

    CALL_OR_DIE(BF_AllocateBlock( fileDesc, firstBlock));
    hp_info = (HP_info *) BF_Block_GetData(firstBlock);
    hp_info->FileDescriptor = fileDesc;

    BF_Block_SetDirty(firstBlock);
    CALL_OR_DIE(BF_UnpinBlock(firstBlock));

    BF_Block_Destroy(&firstBlock);

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

    code = BF_OpenFile(fileName, &fileInfo->FileDescriptor);
    if(code != BF_OK){
        return NULL;
    }
    strcpy(fileInfo->FileName, fileName);
    return fileInfo;
}


int HP_CloseFile( HP_info* hp_info ){
    CALL_OR_DIE(BF_CloseFile(hp_info->FileDescriptor));
    remove(hp_info->FileName);
    free(hp_info);
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
    
    if(numberOfBlocks == 1){
        return (SetUpNewBlock(hp_info, record));
    }
    else{
        BF_Block_Init(&block);

        CALL_OR_DIE(BF_GetBlock(hp_info->FileDescriptor, numberOfBlocks - 1, block));
        data = BF_Block_GetData(block);
        memcpy(&block_info, (HP_block_info*)(data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HP_block_info));
        if(block_info.RecordCount ==RECORDS_PER_BLOCK){
            CALL_OR_DIE(BF_UnpinBlock(block));
            BF_Block_Destroy(&block);
            
            return (SetUpNewBlock(hp_info, record));
        }
        records = (Record*)data;
        records[block_info.RecordCount] = record;
        BF_Block_SetDirty(block);
        CALL_OR_DIE(BF_UnpinBlock(block));

        block_info.RecordCount++;
        memcpy( (HP_block_info*)(data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HP_block_info));

        BF_Block_Destroy(&block);
    }
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
    

    blocksSearched = 0;
    fileFound = 0;
    CALL_OR_DIE(BF_GetBlockCounter(hp_info->FileDescriptor, &blocksInFile));


    BF_Block_Init(&block);

    for(counter = 1; counter < blocksInFile; counter++){

        CALL_OR_DIE(BF_GetBlock(hp_info->FileDescriptor, counter, block));
        data = BF_Block_GetData(block);
        memcpy( &block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HP_block_info));
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
    return blocksSearched;
}

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
    memcpy( (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HP_block_info));

    BF_Block_Destroy(&block);

    CALL_OR_DIE(BF_GetBlockCounter(hp_info->FileDescriptor, &numberOfBlocks));
    return numberOfBlocks - 1;

}


