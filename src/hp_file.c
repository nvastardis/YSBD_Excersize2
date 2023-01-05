#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "hp_file.h"
#include "record.h"

#define CALL_BF(call)       \
{                           \
  BF_ErrorCode code = call; \
  if (code != BF_OK) {         \
    BF_PrintError(code);    \
    return HP_ERROR;        \
  }                         \
}

int SetUpNewBlock(HP_info *hp_info, Record record);

int HP_CreateFile(char *fileName){
    int fileDesc;
    void *data;
    HP_info *hp_info;
    BF_Block *firstBlock;

    if(BF_CreateFile(fileName)){
        return -1;
    }
    if(BF_OpenFile(fileName, &fileDesc)){
        return -1;
    }
    BF_Block_Init(&firstBlock);
    if(BF_AllocateBlock( fileDesc, firstBlock)){
        return -1;;
    }

    hp_info = (HP_info *) BF_Block_GetData(firstBlock);
    hp_info->fileDescriptor = fileDesc;

    BF_Block_SetDirty(firstBlock);
    if(BF_UnpinBlock(firstBlock)){
        return -1;
    }
    BF_Block_Destroy(&firstBlock);
    if(BF_CloseFile(fileDesc)){
        return -1;
    }
    return 0;
}

HP_info* HP_OpenFile(char *fileName){
    HP_info *fileInfo;
    fileInfo = malloc(sizeof(HP_info*));
    if(fileInfo == NULL){
        return NULL;
    }
    if(BF_OpenFile(fileName, &fileInfo->fileDescriptor)){
        return NULL;
    }
    fileInfo->fileName = fileName;
    return fileInfo;
}


int HP_CloseFile( HP_info* hp_info ){
    if(BF_CloseFile(hp_info->fileDescriptor)){
        return -1;
    }
    remove(hp_info->fileName);
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
    if(BF_GetBlockCounter(hp_info->fileDescriptor, &numberOfBlocks)){
        return -1;
    }
    
    if(numberOfBlocks == 1){
        return (SetUpNewBlock(hp_info, record));
    }
    else{
        BF_Block_Init(&block);
        if(BF_GetBlock(hp_info->fileDescriptor, numberOfBlocks - 1, block)){
            return -1;
        }
        data = BF_Block_GetData(block);
        memcpy(&block_info, (HP_block_info*)(data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HP_block_info));
        if(block_info.RecordCount ==RECORDS_PER_BLOCK){
            if(BF_UnpinBlock(block)){
                return -1;
            }
            BF_Block_Destroy(&block);
            return (SetUpNewBlock(hp_info, record));
        }
        records = (Record*)data;
        records[block_info.RecordCount] = record;
        BF_Block_SetDirty(block);
        if(BF_UnpinBlock(block)){
            return -1;
        }
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

    blocksSearched = 0;
    fileFound = 0;
    if(BF_GetBlockCounter(hp_info->fileDescriptor, &blocksInFile)){
        return -1;
    }
    
    BF_Block *block;
    HP_block_info block_info;


    BF_Block_Init(&block);
    for(counter = 1; counter < blocksInFile; counter++){
        if(BF_GetBlock(hp_info->fileDescriptor, counter, block)){
            return -1;
        }
        data = BF_Block_GetData(block);
        if(BF_UnpinBlock(block)){
            return -1;
        }
        memcpy( &block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HP_block_info));
        records = (Record*) data;
        for(recordCounter = 0; recordCounter < block_info.RecordCount; recordCounter++){
            if(records[recordCounter].id == value){
                printRecord(records[recordCounter]);
                if(BF_UnpinBlock(block)){
                    return -1;
                }
                BF_Block_Destroy(&block);
                return blocksSearched;
            }
        }
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
    if(BF_AllocateBlock(hp_info->fileDescriptor, block)){
        return -1;
    }
    data = BF_Block_GetData(block);
    blockData = (Record *) data;
    blockData[0] = record;

    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }

    block_info.RecordCount = 1;
    memcpy( (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HP_block_info));
    BF_Block_Destroy(&block);

    if(BF_GetBlockCounter(hp_info->fileDescriptor, &numberOfBlocks)){
        return -1;
    }
    return numberOfBlocks - 1;

}


