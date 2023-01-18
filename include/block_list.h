/* Βοηθητικές δομές προσωρινής αποθήκευσης block id των μπλοκ του πρωτεύοντος ευρετηρίου */

/* Κόμβος λίστας προσωρινής αποθήκευσης block id*/
typedef struct Node{
    int blockId;
    int numberOfAppearences;
    struct Node *NextNode;
} BlockListNode;
/* Λίστα προσωρινής αποθήκευσης block id*/
typedef struct{
    int Length;
    BlockListNode *Head;
} BlockList;

/* Δημιουργλια και αρχικοποίηση λίστας προσωρινής αποθήκευσης block id*/
BlockList* Initialize();

/* Προσθήκη κόμβου στη λίστα προσωρινής αποθήκευσης block id*/
void AddNode(BlockList* blockList, int newBlockId, int numberOfAppearences);

/* Αφαίρεση κόμβου προσωρινής αποθήκευσης block id*/
void RemoveNode(BlockList* blockList, int blockId);

/* Διαγραφή και απελευθέρωση χώρου της λίστας */
void FreeBlockList(BlockList* blockList);