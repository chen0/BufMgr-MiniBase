package bufmgr;

import global.GlobalConst;

public class Clock implements GlobalConst {

    protected FrameDesc[] buffPool;
    protected int current;
    protected int loopLen;

    public Clock(FrameDesc[] bufferPool) {
        buffPool = bufferPool;
        current = 0;
        loopLen = buffPool.length * 2;
    }

    // Choose which frame to replace from the buffer pool
    public int chooseVictim() {
        FrameDesc frame;
        for (int clock = 0; clock < loopLen; clock++) {
            frame = buffPool[current];
            if (! frame.validBit()) {
                return current;
            }
            if (! frame.isPinned()) {
                if (frame.refBit()) {
                    frame.setRefBit(false);
                }
                else {
                    return current;
                }
            }
            current = (++current) % buffPool.length;
        }
        return -1;
    }
}
