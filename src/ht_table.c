#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int SetUpNewBlock(HT_info *ht_info, Record record, int previousBlockId);
int SetUpNewEmptyBlock(HT_info *ht_info);

int hash(int value, int mask);
int HT_CreateFile(char *fileName,  int buckets){
    int fileDesc, counter;
    void *data;
    HT_info *ht_info;
    BF_Block *firstBlock;

    remove(fileName);
    CALL_OR_DIE(BF_CreateFile(fileName));
    CALL_OR_DIE(BF_OpenFile(fileName, &fileDesc));
    BF_Block_Init(&firstBlock);
    CALL_OR_DIE(BF_AllocateBlock( fileDesc, firstBlock));

    ht_info = (HT_info *) BF_Block_GetData(firstBlock);
    ht_info->bucketDefinitionsBlock = 0;
    strcpy(ht_info->fileName, fileName);
    ht_info->numberOfBuckets = buckets;
    ht_info->fileDescriptor = fileDesc;
    for(counter = 0; counter < buckets; counter++){
        ht_info->hashtableMapping[counter] = counter + 1;
    }
    BF_Block_SetDirty(firstBlock);
    CALL_OR_DIE(BF_UnpinBlock(firstBlock));
    

    BF_Block_Destroy(&firstBlock);
    CALL_OR_DIE(BF_CloseFile(fileDesc));
    return 0;
}

HT_info* HT_OpenFile(char *fileName){
    int fileDesc, counter;
    HT_info *data;
    BF_Block *block;
    BF_ErrorCode code;
    
    code = BF_OpenFile(fileName, &fileDesc);
    if (code != BF_OK) {
      BF_PrintError(code);
      return NULL;
    }

    BF_Block_Init(&block);
    code = BF_GetBlock(fileDesc, 0, block);
    if(code != BF_OK){
        return NULL;
    }
    data = (HT_info*)BF_Block_GetData(block);
    data->fileDescriptor = fileDesc;
    BF_Block_SetDirty(block);
    code = BF_UnpinBlock(block);
    if(code != BF_OK){
        BF_PrintError(code);
        return NULL;
    }
    BF_Block_Destroy(&block);
    for(counter = 0; counter < data->numberOfBuckets; counter++){
        if(SetUpNewEmptyBlock(data) != 0){
            return NULL;
        }
    }
    return data;
}


int HT_CloseFile( HT_info* HT_info ){
    int fileDesc = HT_info->fileDescriptor;
    char fileName[15];
    strcpy(fileName, HT_info->fileName);
    CALL_OR_DIE(BF_CloseFile(HT_info->fileDescriptor));
    remove(fileName);
    return 0;
}

int HT_InsertEntry(HT_info* ht_info, Record record){
    int counter,
        numberOfBlocks,
        hashValue;
    void *data;
    BF_Block *block;
    Record *records;
    HT_block_info block_info;

    hashValue = hash(record.id, ht_info->numberOfBuckets);
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(ht_info->fileDescriptor, ht_info->hashtableMapping[hashValue], block));

    data = BF_Block_GetData(block);
    memcpy( &block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HT_block_info));

    if(block_info.RecordCount == RECORDS_PER_BLOCK){
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        if(SetUpNewBlock(ht_info, record, ht_info->hashtableMapping[hashValue]) != 0){
            return -1;
        }
        CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDescriptor, &numberOfBlocks));

        ht_info->hashtableMapping[hashValue] = numberOfBlocks - 1;
        return ht_info->hashtableMapping[hashValue];
    }
    records = (Record*)data;
    records[block_info.RecordCount] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    block_info.RecordCount++;
    memcpy((data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
    BF_Block_Destroy(&block);

    return ht_info->hashtableMapping[hashValue];
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    int hashValue, 
        recordCounter, 
        counter, 
        blocksInFile, 
        recordsSearched, 
        fileFound,
        currentBlockId;
    void *data;
    Record *records;
    BF_Block *block;
    HT_block_info block_info;

    recordsSearched = 0;
    fileFound = 0;    

    CALL_OR_DIE(BF_GetBlockCounter(ht_info->fileDescriptor, &blocksInFile));

    hashValue = hash(*(int *)value, ht_info->numberOfBuckets);
    currentBlockId = ht_info->hashtableMapping[hashValue];

    BF_Block_Init(&block);

    do{
        CALL_OR_DIE(BF_GetBlock(ht_info->fileDescriptor, currentBlockId, block));
        data = BF_Block_GetData(block);
        memcpy( &block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HT_block_info));
        records = (Record*) data;
        for(recordCounter = 0; recordCounter < block_info.RecordCount; recordCounter++){
            if(records[recordCounter].id == (*(int *)value)){
                CALL_OR_DIE(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
                printRecord(records[recordCounter]);
                return recordsSearched;
            }
            recordsSearched++;
        }
        CALL_OR_DIE(BF_UnpinBlock(block));
        currentBlockId = block_info.PreviousBlockId;
    }while(currentBlockId != 0);

    BF_Block_Destroy(&block);
    return -1;

}

int hash(int value, int mask){
    return value % mask;
}

int SetUpNewEmptyBlock(HT_info *ht_info){
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    HT_block_info block_info;

    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(ht_info->fileDescriptor, block));
    data = BF_Block_GetData(block);

    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount = 0;
    block_info.PreviousBlockId = -1;
    memcpy((data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
    BF_Block_Destroy(&block);
    return 0;
}

int SetUpNewBlock(HT_info *ht_info, Record record, int previousBlockId){
    
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    Record *blockData;
    HT_block_info block_info;

    BF_Block_Init(&block);
    CALL_OR_DIE(BF_AllocateBlock(ht_info->fileDescriptor, block));
    data = BF_Block_GetData(block);
    blockData = (Record *) data;
    blockData[0] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount = 1;
    block_info.PreviousBlockId = previousBlockId;
    memcpy((data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
    BF_Block_Destroy(&block);
    return 0;

}



