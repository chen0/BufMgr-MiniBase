package bufmgr;

import global.Page;
import global.PageId;

class FrameDesc extends Page {

    protected PageId pageNo;
    protected int pinCount;
    protected boolean dirtyBit;
    protected boolean refBit;
    protected boolean validBit;

    public FrameDesc() {
        pageNo = new PageId();
        dirtyBit = false;
        refBit = false;
        validBit = false;
        pinCount = 0;
    }

    public void initAndPin(PageId pageno) {
        pageNo.copyPageId(pageno);
        dirtyBit = false;
        validBit = true;
        refBit = true;
        pinCount = 1;
    }

    public boolean isPinned() {
        return (pinCount > 0);
    }

    // Used in BufMgr.pinPage
    public void incPinCount() { ++pinCount; }

    // Used in BufMgr.pinPage
    public void decPinCount() { --pinCount; }

    public void setDirtyBit(boolean dirty) {
        dirtyBit = dirty;
    }

    public void setValidBit(boolean bit) {
        validBit = bit;
    }

    public void setRefBit(boolean ref) {
        refBit = ref;
    }

    public boolean dirtyBit() { return dirtyBit; }

    public boolean refBit() { return refBit; }

    public boolean validBit() { return validBit; }

    public PageId pageId() { return pageNo; }


}