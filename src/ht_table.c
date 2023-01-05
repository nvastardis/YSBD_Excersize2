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

int hash(int value);
int HT_CreateFile(char *fileName,  int buckets){
    int fileDesc, counter;
    void *data;
    HT_info *ht_info;
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

    ht_info = (HT_info *) BF_Block_GetData(firstBlock);
    ht_info->bucketDefinitionsBlock = 0;
    ht_info->fileDescriptor = fileDesc;
    ht_info->fileName = fileName;
    ht_info->numberOfBuckets = MAX_NUMBER_OF_BUCKETS;
    for(counter = 0; counter < MAX_NUMBER_OF_BUCKETS; counter++){
        if(SetUpNewEmptyBlock(ht_info)){
            return -1;
        }        
        ht_info->hashtableMapping[counter] = counter + 1;
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

HT_info* HT_OpenFile(char *fileName){
    int fileDesc;
    HT_info *data;
    BF_Block *block;

    if(BF_OpenFile(fileName, &fileDesc)){
        return NULL;
    }
    BF_Block_Init(&block);
    if(BF_GetBlock(fileDesc, 0, block)){
        return NULL;
    }
    data = (HT_info*)BF_Block_GetData(block);
    if(BF_UnpinBlock(block)){
        return NULL;
    }
    BF_Block_Destroy(&block);
    return data;
}


int HT_CloseFile( HT_info* HT_info ){
    int fileDesc = HT_info->fileDescriptor;
    char *fileName = HT_info->fileName;
    if(BF_CloseFile(HT_info->fileDescriptor)){
        return -1;
    }
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
    if(BF_GetBlockCounter(ht_info->fileDescriptor, &numberOfBlocks)){
        return -1;
    }

    hashValue = hash(record.id);
    BF_Block_Init(&block);
    if(BF_GetBlock(ht_info->fileDescriptor, ht_info->hashtableMapping[hashValue], block)){
        BF_PrintError(BF_GetBlock(ht_info->fileDescriptor, ht_info->hashtableMapping[hashValue], block));
        return -1;
    }

    data = BF_Block_GetData(block);
    memcpy(&block_info, (HT_block_info*)(data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HT_block_info));

    if(block_info.RecordCount ==RECORDS_PER_BLOCK){
        if(BF_UnpinBlock(block)){
            return -1;
        }
        BF_Block_Destroy(&block);
        if(SetUpNewBlock(ht_info, record, ht_info->hashtableMapping[hashValue]) == -1){
            return -1;
        }
        if(BF_GetBlockCounter(ht_info->fileDescriptor, &numberOfBlocks)){
            return -1;
        }

        ht_info->hashtableMapping[hashValue] = numberOfBlocks - 1;
        return ht_info->hashtableMapping[hashValue];
    }

    records = (Record*)data;
    records[block_info.RecordCount] = record;
    BF_Block_SetDirty(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }
    block_info.RecordCount++;
    memcpy( (HT_block_info*)(data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
    BF_Block_Destroy(&block);

    return ht_info->hashtableMapping[hashValue];
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    int hashValue, 
        recordCounter, 
        counter, 
        blocksInFile, 
        blocksSearched, 
        fileFound,
        currentBlockId;
    void *data;
    Record *records;
    BF_Block *block;
    HT_block_info block_info;

    blocksSearched = 0;
    fileFound = 0;
    if(BF_GetBlockCounter(ht_info->fileDescriptor, &blocksInFile)){
        return -1;
    }
    

    hashValue = hash(*(int *)value);
    currentBlockId = ht_info->hashtableMapping[hashValue];

    BF_Block_Init(&block);

    do{
        if(BF_GetBlock(ht_info->fileDescriptor, currentBlockId, block)){
            return -1;
        }
        data = BF_Block_GetData(block);
        if(BF_UnpinBlock(block)){
            return -1;
        }
        blocksSearched++;
        memcpy( &block_info, (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), sizeof(HT_block_info));
        records = (Record*) data;
        for(recordCounter = 0; recordCounter < block_info.RecordCount; recordCounter++){
            if(records[recordCounter].id == (*(int *)value)){
                if(BF_UnpinBlock(block)){
                    return -1;
                }
                BF_Block_Destroy(&block);
                printRecord(records[recordCounter]);
                return blocksSearched;
            }
        }
        currentBlockId = block_info.PreviousBlockId;
    }while(currentBlockId != -1);

    BF_Block_Destroy(&block);
    return blocksSearched;

}

int hash(int value){
    return value % MAX_NUMBER_OF_BUCKETS ;
}

int SetUpNewEmptyBlock(HT_info *ht_info){
    int counter,
        numberOfBlocks;
    void *data;
    BF_Block *block;
    HT_block_info block_info;

    BF_Block_Init(&block);
    if(BF_AllocateBlock(ht_info->fileDescriptor, block)){
        return -1;
    }
    data = BF_Block_GetData(block);
    if(BF_UnpinBlock(block)){
        return -1;
    }

    block_info.RecordCount = 0;
    block_info.PreviousBlockId = -1;
    memcpy( (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
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
    if(BF_AllocateBlock(ht_info->fileDescriptor, block)){
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
    block_info.PreviousBlockId = previousBlockId;
    memcpy( (data + (RECORDS_PER_BLOCK * sizeof(Record)) + 10), &block_info, sizeof(HT_block_info));
    BF_Block_Destroy(&block);
    return 0;

}



