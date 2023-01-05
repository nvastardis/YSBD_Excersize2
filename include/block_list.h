typedef struct Node{
    int blockId;
    struct Node *NextNode;
} BlockListNode;

typedef struct{
    int Length;
    BlockListNode *Head;
} BlockList;

BlockList* Initialize();
void AddNode(BlockList* blockList, int newBlockId);
void RemoveNode(BlockList* blockList, int blockId);
void FreeBlockList(BlockList* blockList);