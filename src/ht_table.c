#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bf.h"
#include "ht_table.h"
#include "record.h"

#define CALL_OR_DIE(call)      \
{                              \
    BF_ErrorCode code = call;  \
    if (code != BF_OK) {       \
        BF_PrintError(code);   \
        return -1;             \
    }                          \
}

#define CALL_OR_DIE_POINTER(call) \
{                                 \
    BF_ErrorCode code = call;     \
    if (code != BF_OK) {          \
        BF_PrintError(code);      \
        return NULL;              \
    }                             \
}


int SetUpNewBlock(HT_info *ht_info, Record record, int previousBlockId);
int hash(int value, int mask);

int HT_CreateFile(char *fileName,  int buckets){
    int fileDesc, counter;
    void *data;
    HT_info *ht_info;
    BF_Block *firstBlock;

    /* Διαγραφή αρχείου για το hash table implementation, που μπορεί να έχει απομείνει από προηγούμενη εκτέλεση, δημιουργεία και άνοιγμα νέου αρχείου ht*/
    remove(fileName);
    CALL_OR_DIE(BF_CreateFile(fileName));
    CALL_OR_DIE(BF_OpenFile(fileName, &fileDesc));

    /* Δημιουργία πρώτου μπλοκ και αποθήκευση μεταδεδομένων του ht σε αυτό*/
    BF_Block_Init(&firstBlock);

    CALL_OR_DIE(BF_AllocateBlock( fileDesc, firstBlock));
    ht_info = (HT_info *) BF_Block_GetData(firstBlock);

    ht_info->BucketDefinitionsBlock = 0;
    ht_info->NumberOfBuckets = buckets;
    ht_info->FileDescriptor = fileDesc;

    if(buckets > MAX_NUMBER_OF_BUCKETS){
        printf("Max number of buckets allowed is lower than the number provided.\nMax number of buckets is used instead\n");
        buckets = MAX_NUMBER_OF_BUCKETS;
    }

    /* Αρχειοθέτη πίνακα Κάδων του Hash Table */
    ht_info->HashtableMapping = malloc(buckets * sizeof(Bucket_Info));
    for(counter = 0; counter < buckets; counter++){
        ht_info->HashtableMapping[counter].CorrespondingBlock = -1;
        ht_info->HashtableMapping[counter].NumberOfBlocks = 0;
        ht_info->HashtableMapping[counter].NumberOfRecords = 0;
    }

    BF_Block_SetDirty(firstBlock);
    CALL_OR_DIE(BF_UnpinBlock(firstBlock));
    
    BF_Block_Destroy(&firstBlock);
    
    /* Κλείσιμο αρχείου */
    CALL_OR_DIE(BF_CloseFile(fileDesc));
    return 0;
}

HT_info* HT_OpenFile(char *fileName){
    int fileDesc, counter;
    HT_info *data;
    BF_Block *block;
    BF_ErrorCode code;
    
    /* Άνοιγμα του αρχείου και ενημέρωση του fileDescriptor*/
    CALL_OR_DIE_POINTER(BF_OpenFile(fileName, &fileDesc));

    BF_Block_Init(&block);

    CALL_OR_DIE_POINTER(BF_GetBlock(fileDesc, 0, block));

    data = (HT_info*)BF_Block_GetData(block);
    data->FileDescriptor = fileDesc;
    BF_Block_SetDirty(block);
    
    CALL_OR_DIE_POINTER(BF_UnpinBlock(block));
    
    BF_Block_Destroy(&block);
    return data;
}


int HT_CloseFile( HT_info* ht_info ){
    /* Κλείσιμο αρχείου */
    CALL_OR_DIE(BF_CloseFile(ht_info->FileDescriptor));
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

    hashValue = hash(record.id, ht_info->NumberOfBuckets);
    
    /* Αν δεν έχει οριστεί ακόμα μπλοκ στον κάδο, δημιουργία καινούριου μπλοκ και αποθήκευση νεας εγγραφής*/
    if(ht_info->HashtableMapping[hashValue].CorrespondingBlock == -1){
        return SetUpNewBlock(ht_info, record, ht_info->HashtableMapping[hashValue].CorrespondingBlock);
    }
    
    /* Φόρτωση μπλοκ που αντιστοιχεί στον κάδο της εγγραφής*/
    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(ht_info->FileDescriptor, ht_info->HashtableMapping[hashValue].CorrespondingBlock, block));
    data = BF_Block_GetData(block);
    memcpy( &block_info, (data + BF_BLOCK_SIZE - sizeof(HT_block_info)), sizeof(HT_block_info));

    /* Αν ειναι γεμάτο, δημιουργία νέου μπλοκ για τον κάδο και αποθήκευση νέας εγγραφής */
    if(block_info.RecordCount == RECORDS_PER_BLOCK){
        CALL_OR_DIE(BF_UnpinBlock(block));
        BF_Block_Destroy(&block);
        return SetUpNewBlock(ht_info, record, ht_info->HashtableMapping[hashValue].CorrespondingBlock);
    }

    /* Αποθήκευση νέας εγγραφής και ενημέρωση μεταδεδομένων του μπλοκ και του bucket*/
    records = (Record*)data;
    records[block_info.RecordCount] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount++;
    memcpy((data + BF_BLOCK_SIZE - sizeof(HT_block_info)), &block_info, sizeof(HT_block_info));
    
    ht_info->HashtableMapping[hashValue].NumberOfRecords ++;

    CALL_OR_DIE(BF_GetBlock(ht_info->FileDescriptor, ht_info->BucketDefinitionsBlock, block));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));
    BF_Block_Destroy(&block);
    return ht_info->HashtableMapping[hashValue].CorrespondingBlock;
}

int HT_GetAllEntries(HT_info* ht_info, void *value ){
    int hashValue, 
        recordCounter, 
        counter, 
        blocksSearched, 
        fileFound,
        currentBlockId;
    void *data;
    Record *records;
    BF_Block *block;
    HT_block_info block_info;

    blocksSearched = 0;
    fileFound = 0;
    hashValue = hash(*(int *)value, ht_info->NumberOfBuckets);
    /* Εύρεση του κάδου στον οποίο αντιστοιχεί η εγγραφή */
    currentBlockId = ht_info->HashtableMapping[hashValue].CorrespondingBlock;

    BF_Block_Init(&block);

    /* Για κάθε μπλοκ που αντιστοιχούν στον κάδο, αναζήτηση εντός των εγγραφών του για την εγγραφή.
    Μόλις βρεθεί, τυπώνεται η εγγραφή και επιστρέφεται ο αριθμός των blocks. Αν δε βρεθεί η εγγραφή απλά τυπώνεται σχετικό μήνυμα. */
    do{
        CALL_OR_DIE(BF_GetBlock(ht_info->FileDescriptor, currentBlockId, block));
        data = BF_Block_GetData(block);
        memcpy( &block_info, (data + BF_BLOCK_SIZE - sizeof(HT_block_info)), sizeof(HT_block_info));
        records = (Record*) data;
        
        for(recordCounter = 0; recordCounter < block_info.RecordCount; recordCounter++){
            if(records[recordCounter].id == (*(int *)value)){
                CALL_OR_DIE(BF_UnpinBlock(block));
                BF_Block_Destroy(&block);

                printRecord(records[recordCounter]);
                
                return blocksSearched;
            }
        }
        
        CALL_OR_DIE(BF_UnpinBlock(block));
        blocksSearched++;
        currentBlockId = block_info.PreviousBlockId;
    
    }while(currentBlockId != 0);
    BF_Block_Destroy(&block);
    if(blocksSearched == ht_info->HashtableMapping[hashValue].NumberOfBlocks){
        printf("Record with id: %d not found", *(int *)value );
    }
    return blocksSearched;

}

/* Εκτύπωση στατιστικών του πρωτεύοντος ευρετηρίου με τη βοήθεια των μεταδεδομένων του αρχείου και των κάδων */
int HT_HashStatistics( char* filename){
    int fileDesc,
        numberOfBlocks,
        counter,
        mostItemsInBucket,
        leastItemsInBucket,
        numberOfOverFlownBuckets,
        itemSum;
    double average;
    HT_info *data;
    BF_Block *block;

    printf("\n\n--Hash Table statistics For HT Functionality--\n\n");
    printf("Filename:  %s\n", filename);

    CALL_OR_DIE(BF_OpenFile(filename, &fileDesc));

    CALL_OR_DIE(BF_GetBlockCounter(fileDesc, &numberOfBlocks));
    printf("Number of Blocks In File:  %d\n", numberOfBlocks);

    BF_Block_Init(&block);
    CALL_OR_DIE(BF_GetBlock(fileDesc, 0, block));
    data = (HT_info *)BF_Block_GetData(block);

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

/* Βοηθητική συνάρτηση δημιουργίας νέου μπλοκ και ενημέρωσης μεταδεδομένων του κάδου */
int SetUpNewBlock(HT_info *ht_info, Record record, int previousBlockId){
    int counter,
        numberOfBlocks,
        hashValue;
    void *data;
    BF_Block *block;
    Record *blockData;
    HT_block_info block_info;

    hashValue = hash(record.id, ht_info->NumberOfBuckets);

    BF_Block_Init(&block);

    CALL_OR_DIE(BF_AllocateBlock(ht_info->FileDescriptor, block));
    data = BF_Block_GetData(block);
    blockData = (Record *) data;
    blockData[0] = record;
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    block_info.RecordCount = 1;
    block_info.PreviousBlockId = previousBlockId;
    memcpy((data + BF_BLOCK_SIZE - sizeof(HT_block_info)), &block_info, sizeof(HT_block_info));

    CALL_OR_DIE(BF_GetBlockCounter(ht_info->FileDescriptor, &numberOfBlocks));
    
    ht_info->HashtableMapping[hashValue].CorrespondingBlock = numberOfBlocks - 1;
    ht_info->HashtableMapping[hashValue].NumberOfBlocks ++;
    ht_info->HashtableMapping[hashValue].NumberOfRecords ++;

    CALL_OR_DIE(BF_GetBlock(ht_info->FileDescriptor, ht_info->BucketDefinitionsBlock, block));
    BF_Block_SetDirty(block);
    CALL_OR_DIE(BF_UnpinBlock(block));

    BF_Block_Destroy(&block);
    return ht_info->HashtableMapping[hashValue].CorrespondingBlock;
}

int hash(int value, int mask){
    return value % mask;
}