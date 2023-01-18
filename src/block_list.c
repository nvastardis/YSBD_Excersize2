#include <stdio.h>
#include <stdlib.h>
#include "block_list.h"

BlockList* Initialize(){
    BlockList *blockList;
    blockList = malloc(sizeof(BlockList));
    if(blockList == NULL){
        return NULL;
    }
    blockList->Head = NULL;
    blockList->Length = 0;
    return blockList;
}

void AddNode(BlockList* blockList, int newBlockId, int numberOfAppearences){
    BlockListNode *newNode, *traverser;
    newNode = malloc(sizeof(BlockListNode));
    if(newNode == NULL){
        return;
    }
    newNode->blockId = newBlockId;
    newNode->numberOfAppearences = numberOfAppearences;
    newNode->NextNode = NULL;

    if(blockList->Head != NULL){
        traverser = blockList->Head;
        while(traverser->NextNode != NULL){
            if(traverser->blockId == newBlockId){
                free(newNode);
                return;
            }
            traverser = traverser->NextNode;
        }
        traverser->NextNode = newNode;
    }
    else{
        blockList->Head = newNode;
    }
    blockList->Length++;
}

void RemoveNode(BlockList* blockList, int blockId){
    int itemFoundFlag;
    BlockListNode *currentNode, *previousNode, *nextNode;

    if(blockList->Head == 0){
        return;
    }
    itemFoundFlag = 0;
    currentNode = blockList->Head;
    previousNode = NULL;

    while(currentNode != NULL){
        if(currentNode->blockId == blockId){
            itemFoundFlag = 1;
            break;
        }
        previousNode = currentNode;
        currentNode = currentNode->NextNode;
    }
    if(itemFoundFlag == 0){
        return;
    }

    previousNode->NextNode = currentNode->NextNode;
    blockList->Head--;
    currentNode->NextNode = NULL;
    free(currentNode);
    return;
}

void FreeBlockList(BlockList* blockList){
    BlockListNode* traverser, *tmp;

    if(blockList->Length > 0){
        traverser = blockList->Head;
        while (traverser != NULL){
            tmp = traverser;
            traverser = traverser->NextNode;
            free(tmp);
        }
    }
    free(blockList);
    return;
}