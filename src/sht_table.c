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
        return -1;             \
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

int SetUpNewBlockInSht(SHT_info *Sht_info, SHT_Record record, int previousBlockId);
int hashName(char *value, int numberOfBuckets);

int SHT_CreateSecondaryIndex(char *sfileName,  int buckets, char* fileName){
    int fileDesc, counter;
    void *data;
    SHT_info *sht_info;
    BF_Block *firstBlock;

    /* Διαγραφή αρχείου για το secondary hash table implementation(που μπορεί να έχει απομείνει από προηγούμενη εκτέλεση) δημιουργεία και άνοιγμα νέου αρχείου sht*/
    remove(sfileName);
    CALL_OR_DIE(BF_CreateFile(sfileName));
    CALL_OR_DIE(BF_OpenFile(sfileName, &fileDesc));

    /* Δημιουργία πρώτου μπλοκ και αποθήκευση μεταδεδομένων του sht σε αυτό*/
    BF_Block_Init(&firstBlock);
    CALL_OR_DIE(BF_AllocateBlock( fileDesc, firstBlock));
    sht_info = (SHT_info *) BF_Block_GetData(firstBlock);
    
    sht_info->BucketDefinitionsBlock = 0;
    strcpy(sht_info->FileName, sfileName);
    sht_info->NumberOfBuckets = buckets;
    sht_info->FileDescriptor = fileDesc;
    
    if(buckets > MAX_NUMBER_OF_BUCKETS){
        printf("Max number of buckets allowed is lower than the number provided.\nMax number of buckets is used instead\n");
        buckets = MAX_NUMBER_OF_BUCKETS;
    }
    
    /* Αρχειοθέτη πίνακα Κάδων του Hash Table */
    sht_info->HashtableMapping = malloc(buckets * sizeof(Bucket_Info));
    for(counter = 0; counter < buckets; counter++){
        sht_info->HashtableMapping[counter].CorrespondingBlock = -1;
        sht_info->HashtableMapping[counter].NumberOfBlocks = 0;
        sht_info->HashtableMapping[counter].NumberOfRecords = 0;
    }

    BF_Block_SetDirty(firstBlock);
    CALL_OR_DIE(BF_UnpinBlock(firstBlock));
    
    BF_Block_Destroy(&firstBlock);
    /* Κλείσιμο αρχείου */
    CALL_OR_DIE(BF_CloseFile(fileDesc));
    return 0;
}

SHT_info* SHT_OpenSecondaryIndex(char *indexName){
    int fileDesc, counter;
    BF_ErrorCode code;
    SHT_info *data;
    BF_Block *block;

    /* Άνοιγμα του αρχείου και ενημέρωση του fileDescriptor*/
    CALL_OR_DIE_POINTER(BF_OpenFile(indexName, &fileDesc));

    BF_Block_Init(&block);
    
    CALL_OR_DIE_POINTER(BF_GetBlock(fileDesc, 0, block));
    data = (SHT_info*)BF_Block_GetData(block);
    data->FileDescriptor = fileDesc;
    BF_Block_SetDirty(block);
    CALL_OR_DIE_POINTER(BF_UnpinBlock(block));
    
    BF_Block_Destroy(&block);
    return data;
}


int SHT_CloseSecondaryIndex( SHT_info* SHT_info ){
    /* Κλείσιμο αρχείου */
    CALL_OR_DIE(BF_CloseFile(SHT_info->FileDescriptor));
    return 0;
}

int SHT_SecondaryInsertEntry(SHT_info* sht_info, Record record, int block_id){
    int counter,
        numberOfBlocks,
        hashValue,
        currentBlockId;
    void *data;
    BF_Block *block;
    SHT_Record *shtRecords, newShtRecord;
    SHT_block_info block_info;

    /* Δημιουργία εγγραφής για το δευτερεύων ευρετήριο με βάση την εγγραφή του πρωτεύοντος */
    strcpy(newShtRecord.name, record.name);
    newShtRecord.blockId = block_id;
    newShtRecord.numberOfAppearences = 0;
    hashValue = hashName(newShtRecord.name, sht_info->NumberOfBuckets);
    
    /* Αν δεν έχει οριστεί ακόμα μπλοκ στον κάδο, δημιουργία καινούριου μπλοκ και αποθήκευση νεας εγγραφής*/
    if(sht_info->HashtableMapping[hashValue].CorrespondingBlock == -1){
        return SetUpNewBlockInSht(sht_info, newShtRecord, sht_info->HashtableMapping[hashValue].CorrespondingBlock);
    }

    /* Φόρτωση μπλοκ που αντιστοιχεί στον κάδο της εγγραφής*/
    BF_Block_Init(&block);
    currentBlockId = sht_info->HashtableMapping[hashValue].CorrespondingBlock;

    /* Αν βρεθεί σε ένα από τα μπλοκ που έχουν αντιστοιχιθεί στον κάδο, εγγρφή με τα ίδια στοιχεία, ενημέρωση του αριθμού εμφανίσεων του ονόματος στο ίδιο μπλοκ,
    στην εγγραφή του δευτερεύοντος ευρετηρίου και επιστροφή */
    do{
        CALL_OR_DIE(BF_GetBlock(sht_info->FileDescriptor, currentBlockId, block));
        data = BF_Block_GetData(block);
        memcpy(&block_info, (data + BF_BLOCK_SIZE - sizeof(SHT_block_info)), sizeof(SHT_block_info));
        shtRecords = (SHT_Record*)data;
        for(counter = 0; counter < block_info.RecordCount; counter++){
            if((shtRecords[counter].blockId == block_id) && (strcmp(shtRecords[counter].name, record.name) == 0)){
                shtRecords[counter].numberOfAppearences++;
                BF_Block_SetDirty(block);
                CALL_OR_DIE(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);
                return currentBlockId;
            }
        }        
        CALL_OR_DIE(BF_UnpinBlock(block));
        currentBlockId = block_info.PreviousBlockId;
    }while (currentBlockId != -1);
    
    /* Φόρτωση μπλοκ που αντιστοιχεί στον κάδο της εγγραφής*/
    currentBlockId = sht_info->HashtableMapping[hashValue].CorrespondingBlock;
    CALL_OR_DIE(BF_GetBlock(sht_info->FileDescriptor, currentBlockId, block));
    data = BF_Block_GetData(block);
    memcpy(&block_info, (data + BF_BLOCK_SIZE - sizeof(SHT_block_info)), sizeof(SHT_block_info));
    
    /* Αν ειναι γεμάτο, δημιουργία νέου μπλοκ για τον κάδο και αποθήκευση νέας εγγραφής */
    if(block_info.RecordCount >= SHT_RECORDS_PER_BLOCK){
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        return SetUpNewBlockInSht(sht_info, newShtRecord, sht_info->HashtableMapping[hashValue].CorrespondingBlock);
    }
    
    /* Αποθήκευση νέας εγγραφής και ενημέρωση μεταδεδομένων του μπλοκ και του bucket*/
    shtRecords = (SHT_Record*)data;
    shtRecords[block_info.RecordCount] = newShtRecord;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount++;
    memcpy( (data + BF_BLOCK_SIZE - sizeof(SHT_block_info)), &block_info, sizeof(SHT_block_info));
    
    sht_info->HashtableMapping[hashValue].NumberOfRecords ++;

    CALL_OR_DIE(BF_GetBlock(sht_info->FileDescriptor, sht_info->BucketDefinitionsBlock, block));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);

    return sht_info->HashtableMapping[hashValue].CorrespondingBlock;
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
    hashValue = hashName(name, sht_info->NumberOfBuckets);
    /* Εύρεση του κάδου στον οποίο αντιστοιχεί η εγγραφή */
    currentBlockId = sht_info->HashtableMapping[hashValue].CorrespondingBlock;
    
    BF_Block_Init(&block);

    /* Για κάθε μπλοκ που αντιστοιχούν στον κάδο, αναζήτηση εντός των εγγραφών του για το όνομα που ζητήθηκε. 
    Κάθε φορά που θα εντοπίζεται, αποθήκευση του αντιστοιχισμένου μπλοκ στο πρωτεύον ευρετήριο και του αριθμού εμφανίσεων του, στην προσωρινή λίστα αποθήκευσης των block id*/
    do{
        SHT_block_info sht_block_info;

        CALL_OR_DIE(BF_GetBlock(sht_info->FileDescriptor, currentBlockId, block));
        data = BF_Block_GetData(block);
        memcpy( &sht_block_info, (data + BF_BLOCK_SIZE - sizeof(SHT_block_info)), sizeof(SHT_block_info));
        sht_BlockData = (SHT_Record*) data;
        
        for(recordCounter = 0; recordCounter < sht_block_info.RecordCount; recordCounter++){
            if(strcmp(sht_BlockData[recordCounter].name, name) == 0){
                AddNode(resultList, sht_BlockData[recordCounter].blockId, sht_BlockData[recordCounter].numberOfAppearences);
            }
        
        }

        CALL_OR_DIE(BF_UnpinBlock(block));
        blocksSearched++;
        currentBlockId = sht_block_info.PreviousBlockId;

    }while(currentBlockId != -1);

    /* Αν η λίστα προσωρινής αποθήκευσης είναι κενή, δε βρέθηκε όνομα με τη συγκεκριμένη εγγραφή */
    if(resultList->Length == 0){
        printf("Record with name: %s not found", name );
        return blocksSearched;
    }
    
    /* Για κάθε block_id στην προσωρινή λίστα, φόρτωση του αντίστοιχου μπλοκ από το πρωτεύον ευρετήριο.
    Αναζήτση στις εγγρφές τους για το αναζητούμενο όνομα και εντύπωση των αντίστοιχων εγγραφών*/
    trv = resultList->Head;
    while(trv != NULL){
        HT_block_info ht_block_info;
        int resultsFounOnCurrentBlock;
        
        resultsFounOnCurrentBlock = 0;


        CALL_OR_DIE(BF_GetBlock(ht_info->FileDescriptor, trv->blockId, block));
        data = BF_Block_GetData(block);
        memcpy( &ht_block_info, (data + BF_BLOCK_SIZE - sizeof(HT_block_info)), sizeof(HT_block_info));
        ht_BlockData = (Record*) data;

        for(recordCounter = 0; recordCounter < ht_block_info.RecordCount || resultsFounOnCurrentBlock < trv->numberOfAppearences; recordCounter++){
            if(strcmp(ht_BlockData[recordCounter].name, name) == 0){
                printRecord(ht_BlockData[recordCounter]);
                resultsFounOnCurrentBlock++;
            }
        }

        CALL_OR_DIE(BF_UnpinBlock(block));
        trv = trv->NextNode;

    }

    BF_Block_Destroy(&block);
    FreeBlockList(resultList);

    return blocksSearched;
}




int SHT_HashStatistics( char* filename){
    int fileDesc,
        numberOfBlocks,
        counter,
        mostItemsInBucket,
        leastItemsInBucket,
        numberOfOverFlownBuckets,
        itemSum;
    double average;
    SHT_info *data;
    BF_Block *block;

    printf("\n\n--Hash Table statistics For SHT Functionality--\n\n");
    printf("Filename:  %s\n", filename);

    CALL_OR_DIE(BF_OpenFile(filename, &fileDesc));

    CALL_OR_DIE(BF_GetBlockCounter(fileDesc, &numberOfBlocks));
    printf("Number of Blocks In File:  %d\n", numberOfBlocks);

    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fileDesc, 0, block));
    data = (SHT_info *)BF_Block_GetData(block);

    mostItemsInBucket = 0;
    leastItemsInBucket = RAND_MAX;
    itemSum = 0;
    for(counter = 0; counter < data->NumberOfBuckets; counter++){
        int recordCounter;
        
        recordCounter = data->HashtableMapping[counter].NumberOfRecords;
        if(recordCounter < leastItemsInBucket){
            leastItemsInBucket = recordCounter;
        }
        if(recordCounter > mostItemsInBucket){
            mostItemsInBucket = recordCounter;
        }
        itemSum+=recordCounter;
    }
    average = itemSum / (double)data->NumberOfBuckets;
    printf("Most Records in a bucket:  %d\n", mostItemsInBucket);
    printf("Least Records in a bucket:  %d\n", leastItemsInBucket);
    printf("Average number of records in a bucket:  %f\n", average);

    itemSum = 0;
    for(counter = 0; counter < data->NumberOfBuckets; counter++){
        int blockCounter;
        
        blockCounter = data->HashtableMapping[counter].NumberOfBlocks;
        itemSum+= blockCounter;
    }
    average = itemSum / (double)data->NumberOfBuckets;
    printf("Average number of blocks in a bucket:  %f\n", average);
    
    numberOfOverFlownBuckets = 0;
    for(counter = 0; counter < data->NumberOfBuckets; counter++){
        int numberOfBlocksInBucket;
        
        numberOfBlocksInBucket = data->HashtableMapping[counter].NumberOfBlocks;
        if(numberOfBlocksInBucket > 1){
            numberOfOverFlownBuckets++;
        }
    }
    printf("Number Of Overflown Buckets:  %d\nSpecifically:\n", numberOfOverFlownBuckets);

    for(counter = 0; counter < data->NumberOfBuckets; counter++){
        int numberOfBlocksInBucket;
        
        numberOfBlocksInBucket = data->HashtableMapping[counter].NumberOfBlocks;
        if(numberOfBlocksInBucket > 1){
            printf("  Bucket:  %d\n    Number of Overflow Blocks:  %d\n", counter, numberOfBlocksInBucket - 1);
        }
    }

    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    printf("\n\n--End Of Report--\n\n");
    return 0;
}

int SetUpNewBlockInSht(SHT_info *sht_info, SHT_Record record, int previousBlockId){
    
    int counter,
        numberOfBlocks,
        hashValue;
    void *data;
    BF_Block *block;
    SHT_Record *blockData;
    SHT_block_info block_info;

    hashValue = hashName(record.name, sht_info->NumberOfBuckets);

    BF_Block_Init(&block);

    CALL_OR_DIE(BF_AllocateBlock(sht_info->FileDescriptor, block));
    data = BF_Block_GetData(block);
    blockData = (SHT_Record *) data;
    blockData[0] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount = 1;
    block_info.PreviousBlockId = previousBlockId;
    memcpy( (data + BF_BLOCK_SIZE - sizeof(SHT_block_info)), &block_info, sizeof(SHT_block_info));

    CALL_OR_DIE(BF_GetBlockCounter(sht_info->FileDescriptor, &numberOfBlocks));
    
    sht_info->HashtableMapping[hashValue].CorrespondingBlock = numberOfBlocks - 1;
    sht_info->HashtableMapping[hashValue].NumberOfBlocks ++;
    sht_info->HashtableMapping[hashValue].NumberOfRecords ++;

    CALL_OR_DIE(BF_GetBlock(sht_info->FileDescriptor, sht_info->BucketDefinitionsBlock, block));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
    return sht_info->HashtableMapping[hashValue].CorrespondingBlock;
}

int hashName(char *value, int numberOfBuckets){
    int i, sum, limit;
    limit = strlen(value);
    sum = 0;
    for(i = 0; i < limit; i++){
        sum += (int)(value[i]);
    }
    return sum % numberOfBuckets;    
}