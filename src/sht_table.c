#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "sht_table.h"
#include "ht_table.h"
#include "record.h"
#include "block_list.h"

#define CALL_OR_DIE(call)     \
  {                           \
    BF_ErrorCode code = call; \
    if (code != BF_OK) {      \
      BF_PrintError(code);    \
      exit(code);             \
    }                         \
  }

int SetUpNewBlockInSht(SHT_info *Sht_info, SHT_Record record, int previousBlockId);
int SetUpNewEmptyBlockInSht(SHT_info *Sht_info);
int hashName(char *value, int numberOfBuckets);

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
    int fileDesc, counter;
    void *data;
    SHT_info *sht_info;
    BF_Block *firstBlock;

    remove(sfileName);

    if(BF_CreateFile(sfileName)){
        return -1;
    }
    if(BF_OpenFile(sfileName, &fileDesc)){
        return -1;
    }
    BF_Block_Init(&firstBlock);
    if(BF_AllocateBlock( fileDesc, firstBlock)){
        return -1;;
    }

    sht_info = (SHT_info *) BF_Block_GetData(firstBlock);
    sht_info->bucketDefinitionsBlock = 0;
    sht_info->fileName = sfileName;
    sht_info->numberOfBuckets = buckets;
    for(counter = 0; counter < buckets; counter++){
        sht_info->hashtableMapping[counter] = counter + 1;
    }

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

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
    int fileDesc, counter;
    SHT_info *data;
    BF_Block *block;

    if(BF_OpenFile(indexName, &fileDesc)){
        return NULL;
    }
    BF_Block_Init(&block);
    if(BF_GetBlock(fileDesc, 0, block)){
        return NULL;
    }
    data = (SHT_info*)BF_Block_GetData(block);
    data->fileDescriptor = fileDesc;
    for(counter = 0; counter < data->numberOfBuckets; counter++){
        if(SetUpNewEmptyBlockInSht(data) == -1){
            return NULL;
        }
    }
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block)){
        return NULL;
    }
    BF_Block_Destroy(&block);
    return data;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
    int fileDesc = SHT_info->fileDescriptor;
    int nameLength = strlen(SHT_info->fileName);
    char *fileName;
    fileName = malloc(sizeof(char) * nameLength + 1);
    strcpy(fileName, SHT_info->fileName);
    if(BF_CloseFile(SHT_info->fileDescriptor)){
        return -1;
    }
    remove(fileName);
    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
    int counter,
        numberOfBlocks,
        hashValue;
    void *data;
    BF_Block *block;
    SHT_Record *blockData, currentRecord;
    SHT_block_info block_info;
    if(BF_GetBlockCounter(sht_info->fileDescriptor, &numberOfBlocks)){
        return -1;
    }
    strcpy(currentRecord.name, record.name);
    currentRecord.blockId = block_id;

    hashValue = hashName(currentRecord.name, sht_info->numberOfBuckets);
    BF_Block_Init(&block);
    if(BF_GetBlock(sht_info->fileDescriptor, sht_info->hashtableMapping[hashValue], block)){
        return -1;
    }

    data = BF_Block_GetData(block);
    memcpy(&block_info, (data + (SHT_RECORDS_PER_BLOCK * sizeof(SHT_Record)) + 10), sizeof(SHT_block_info));

    if(block_info.RecordCount == SHT_RECORDS_PER_BLOCK){
        if(BF_UnpinBlock(block)){
            return -1;
        }
        BF_Block_Destroy(&block);
        if(SetUpNewBlockInSht(sht_info, currentRecord, sht_info->hashtableMapping[hashValue]) == -1){
            return -1;
        }
        if(BF_GetBlockCounter(sht_info->fileDescriptor, &numberOfBlocks)){
            return -1;
        }

        sht_info->hashtableMapping[hashValue] = numberOfBlocks - 1;
        return sht_info->hashtableMapping[hashValue];
    }

    blockData = (SHT_Record*)data;
    blockData[block_info.RecordCount] = currentRecord;
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }
    block_info.RecordCount++;
    memcpy( (SHT_block_info*)(data + (SHT_RECORDS_PER_BLOCK * sizeof(SHT_Record)) + 10), &block_info, sizeof(SHT_block_info));
    BF_Block_Destroy(&block);

    return sht_info->hashtableMapping[hashValue];
}

int SHT_SecondaryGetAllEntries(HT_info* ht_info, SHT_info* sht_info, char* name){
    int hashValue, 
        recordCounter, 
        counter, 
        blocksInFile, 
        blocksSearched, 
        fileFound,
        currentBlockId;
    void *data;
    SHT_Record *sht_BlockData;
    Record *ht_BlockData;
    BF_Block *block;
    BlockList *resultList;
    BlockListNode *trv;
    
    resultList = Initialize();
    if(resultList == NULL){
        return -1;
    }

    blocksSearched = 0;
    fileFound = 0;
    if(BF_GetBlockCounter(sht_info->fileDescriptor, &blocksInFile)){
        return -1;
    }
    
    hashValue = hashName(name, sht_info->numberOfBuckets);
    currentBlockId = sht_info->hashtableMapping[hashValue];
    BF_Block_Init(&block);
    do{
        SHT_block_info sht_block_info;
        if(BF_GetBlock(sht_info->fileDescriptor, currentBlockId, block)){
            return -1;
        }
        data = BF_Block_GetData(block);
        if(BF_UnpinBlock(block)){
            return -1;
        }
        memcpy( &sht_block_info, (data + (SHT_RECORDS_PER_BLOCK * sizeof(SHT_Record)) + 10), sizeof(SHT_block_info));
        sht_BlockData = (SHT_Record*) data;
        for(recordCounter = 0; recordCounter < sht_block_info.RecordCount; recordCounter++){
            if(strcmp(sht_BlockData[recordCounter].name, name) == 0){
                AddNode(resultList, sht_BlockData->blockId);
            }
        }
        blocksSearched++;
        currentBlockId = sht_block_info.PreviousBlockId;
    }while(currentBlockId != -1);

    if(resultList->Length == 0){
        return -1;
    }
    
    trv = resultList->Head;
    while(trv != NULL){
        HT_block_info ht_block_info;
        if(BF_GetBlock(ht_info->fileDescriptor, trv->blockId, block)){
            return -1;
        }
        data = BF_Block_GetData(block);
        if(BF_UnpinBlock(block)){
            return -1;
        }
        memcpy( &ht_block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HT_block_info));
        ht_BlockData = (Record*) data;
        for(recordCounter = 0; recordCounter < ht_block_info.RecordCount; recordCounter++){
            if(strcmp(ht_BlockData[recordCounter].name, name) == 0){
                printRecord(ht_BlockData[recordCounter]);
            }
        }
        blocksSearched++;
        trv = trv->NextNode;
    }
    BF_Block_Destroy(&block);
    FreeBlockList(resultList);
    return blocksSearched;
}




int SetUpNewEmptyBlockInSht(SHT_info *sht_info){
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    SHT_block_info block_info;

    BF_Block_Init(&block);
    if(BF_AllocateBlock(sht_info->fileDescriptor, block)){
        return -1;
    }
    data = BF_Block_GetData(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }

    block_info.RecordCount = 0;
    block_info.PreviousBlockId = -1;
    memcpy( (data + (SHT_RECORDS_PER_BLOCK * sizeof(SHT_Record)) + 10), &block_info, sizeof(SHT_block_info));
    BF_Block_Destroy(&block);
    return 0;
}

int SetUpNewBlockInSht(SHT_info *ht_info, SHT_Record record, int previousBlockId){
    
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    SHT_Record *blockData;
    SHT_block_info block_info;

    BF_Block_Init(&block);
    if(BF_AllocateBlock(ht_info->fileDescriptor, block)){
        return -1;
    }
    data = BF_Block_GetData(block);
    blockData = (SHT_Record *) data;
    blockData[0] = record;
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }

    block_info.RecordCount = 1;
    block_info.PreviousBlockId = previousBlockId;
    memcpy( (data + (SHT_RECORDS_PER_BLOCK * sizeof(SHT_Record)) + 10), &block_info, sizeof(SHT_block_info));
    BF_Block_Destroy(&block);
    return 0;
}

int hashName(char *value, int numberOfBuckets){
    int i, sum, limit;
    limit = strlen(value);
    sum = 0;
    for(i = 0; i < limit; i++){
        sum += (int)(value[i]);
    }
    return sum % MAX_NUMBER_OF_BUCKETS_ON_SHT;    
}





