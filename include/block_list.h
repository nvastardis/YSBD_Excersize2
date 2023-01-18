typedef struct Node{
    int blockId;
    int numberOfAppearences;
    struct Node *NextNode;
} BlockListNode;

typedef struct{
    int Length;
    BlockListNode *Head;
} BlockList;

BlockList* Initialize();
void AddNode(BlockList* blockList, int newBlockId, int numberOfAppearences);
void RemoveNode(BlockList* blockList, int blockId);
void FreeBlockList(BlockList* blockList);