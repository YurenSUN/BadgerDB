/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include <memory>
#include <iostream>
#include "buffer.h"
#include "exceptions/buffer_exceeded_exception.h"
#include "exceptions/page_not_pinned_exception.h"
#include "exceptions/page_pinned_exception.h"
#include "exceptions/bad_buffer_exception.h"
#include "exceptions/hash_not_found_exception.h"

namespace badgerdb
{

/**
 * Constructor of BufMgr class
 * Allocates an array for the buffer pool with bufs page frames and a corresponding BufDesc table
 */
BufMgr::BufMgr(std::uint32_t bufs)
	: numBufs(bufs)
{
	bufDescTable = new BufDesc[bufs];

	for (FrameId i = 0; i < bufs; i++)
	{
		bufDescTable[i].frameNo = i;
		bufDescTable[i].valid = false;
	}

	bufPool = new Page[bufs];

	int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
	hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

	clockHand = bufs - 1;
}

/**
 * Destructor of BufMgr class
 * Flushes out all dirty pages and deallocates the buffer pool and the BufDesc table
 */
BufMgr::~BufMgr()
{
	// flushes out all dirty pages
	for (FrameId i = 0; i < numBufs; i++)
	{
		//TODO: whether check bufDescTable[clockHand].valid
		if(bufDescTable[i].dirty && bufDescTable[i].valid){
			bufDescTable[i].file->writePage(bufPool[i]);
			bufDescTable[i].dirty = false;
		}
	}

	//deallocates the buffer pool
	delete[] bufPool;

	//deallocates the BufDesc table
	delete[] bufDescTable;
}

/**
 * Advance clock to next frame in the buffer pool
 * 
 * @param frame   	Frame reference, frame ID of allocated frame returned via this variable
 * @throws BufferExceededException If no such buffer is found which can be allocated
 */ 
void BufMgr::advanceClock()
{
	clockHand = (clockHand + 1) % numBufs;
}

/**
 * Allocates a free frame
 * if necessary, writing a dirty page back to disk
 */
void BufMgr::allocBuf(FrameId &frame)
{
	//using the clock algorithm
	std::uint32_t pinned = 0;
	advanceClock();

	while (bufDescTable[clockHand].valid)
	{
		if(bufDescTable[clockHand].refbit)
		{
			//clear refbit and advance clock
			bufDescTable[clockHand].refbit = false;
			advanceClock();
			continue;
		}
		
		if(bufDescTable[clockHand].pinCnt)
		{
			//Throws BufferExceededException if all buffer frames are pinned. 
			pinned = pinned + 1;
			if(pinned == numBufs) throw BufferExceededException();
			
			advanceClock();
			continue;
		}

		//have a valid, not referenced, and unpinned page
		//use this frame
		if(bufDescTable[clockHand].dirty)
		{
			//flush page to disk
			bufDescTable[clockHand].file->writePage(bufPool[clockHand]);
			bufDescTable[clockHand].dirty = false;	
		}
		//if the buffer frame allocated has a valid page in it, remove the appropriate entry from the hash table.
		hashTable->remove(bufDescTable[clockHand].file, bufDescTable[clockHand].pageNo);
		break;
	}

	bufDescTable[clockHand].Clear();

	//set the frame ID of allocated frame returned via this variable
	//Set() in the clock algo will be called during readPage() and allocPage()
	frame = clockHand;
}

void BufMgr::readPage(File *file, const PageId pageNo, Page *&page)
{
    try
    {
        hashTable->lookup(file, pageNo, clockHand);
        bufDescTable[clockHand].refbit=true;
        bufDescTable[clockHand].pinCnt++;
        return;

    }catch(HashNotFoundException e)
    {
       FrameId frameId = 0;
       this -> allocBuf(frameId);
       Page new_page = file->readPage(pageNo);
       bufPool[clockHand]= new_page;
       hashTable -> insert(file, pageNo, frameId);
       bufDescTable[clockHand].Set(file, pageNo);
    }
}

void BufMgr::unPinPage(File *file, const PageId pageNo, const bool dirty)
{
     try
     {
       hashTable->lookup(file, pageNo, clockHand);
       if(!bufDescTable[pageNo].pinCnt)
       {
           throw PageNotPinnedException(file->filename(), pageNo, clockHand);
       }
	     
       --bufDescTable[pageNo].pinCnt;
       if (bufDescTable[pageNo].dirty)
       {
           bufDescTable[pageNo].dirty = false;
       }
    }catch (HashNotFoundException e){
    }
}

void BufMgr::flushFile(const File *file)
{
}

void BufMgr::allocPage(File *file, PageId &pageNo, Page *&page)
{
    Page new_page = file->allocatePage();
    FrameId frameId = 0;
    this -> allocBuf(frameId);
    bufPool[clockHand]= new_page;
    hashTable -> insert(file, pageNo, clockHand);
    bufDescTable[clockHand].Set(file, pageNo);
}

void BufMgr::disposePage(File *file, const PageId PageNo)
{
}

void BufMgr::printSelf(void)
{
	BufDesc *tmpbuf;
	int validFrames = 0;

	for (std::uint32_t i = 0; i < numBufs; i++)
	{
		tmpbuf = &(bufDescTable[i]);
		std::cout << "FrameNo:" << i << " ";
		tmpbuf->Print();

		if (tmpbuf->valid == true)
			validFrames++;
	}

	std::cout << "Total Number of Valid Frames:" << validFrames << "\n";
}

} // namespace badgerdb
