package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.LinkedList;
import java.util.List;

/**
 * List commands: LPUSH/RPUSH/LPOP/RPOP/LRANGE/LLEN/LINDEX/LSET/LINSERT/LREM/
 * LTRIM/LPOS/RPOPLPUSH/LMOVE.
 */
public final class ListCommands {

    private ListCommands() {}

    public static void lpush(RedisClient c) { push(c, true); }
    public static void rpush(RedisClient c) { push(c, false); }

    private static void push(RedisClient c, boolean left) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        LinkedList<String> l;
        try {
            l = Values.getOrCreateList(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        boolean wasEmpty = l.isEmpty();
        for (int j = 2; j < c.argc(); j++) {
            if (left) l.addFirst(c.arg(j));
            else      l.addLast(c.arg(j));
        }
        Values.saveList(c.storage(), key, l, null);
        if (wasEmpty) {
            io.github.redis.core.BlockingCoordinator bc =
                    c.services().get(io.github.redis.core.BlockingCoordinator.class);
            if (bc != null) bc.notifyPushedTo(key);
        }
        c.replyInteger(l.size());
    }

    public static void lpushx(RedisClient c) { pushx(c, true); }
    public static void rpushx(RedisClient c) { pushx(c, false); }

    private static void pushx(RedisClient c, boolean left) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        String key = c.arg(1);
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyInteger(0); return; }
        for (int j = 2; j < c.argc(); j++) {
            if (left) l.addFirst(c.arg(j));
            else      l.addLast(c.arg(j));
        }
        Values.saveList(c.storage(), key, l, null);
        c.replyInteger(l.size());
    }

    public static void lpop(RedisClient c) { pop(c, true); }
    public static void rpop(RedisClient c) { pop(c, false); }

    private static void pop(RedisClient c, boolean left) {
        if (c.argc() < 2 || c.argc() > 3) { c.replyWrongArgc(); return; }
        Integer count = null;
        if (c.argc() == 3) {
            count = Args.parseInt(c, 2);
            if (count == null) return;
            if (count < 0) { c.replyError("ERR value is out of range, must be positive"); return; }
        }
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) {
            if (count == null) c.replyNil(); else c.replyEmptyArray();
            return;
        }
        if (count == null) {
            String v = left ? l.removeFirst() : l.removeLast();
            Values.saveList(c.storage(), c.arg(1), l, null);
            c.replyBulk(v);
        } else {
            List<Object> out = new java.util.ArrayList<>(Math.min(count, l.size()));
            for (int i = 0; i < count && !l.isEmpty(); i++) {
                out.add(left ? l.removeFirst() : l.removeLast());
            }
            Values.saveList(c.storage(), c.arg(1), l, null);
            c.replyArray(out);
        }
    }

    public static void llen(RedisClient c) {
        if (c.argc() != 2) { c.replyWrongArgc(); return; }
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        c.replyInteger(l == null ? 0 : l.size());
    }

    public static void lindex(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        Integer idx = Args.parseInt(c, 2);
        if (idx == null) return;
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyNil(); return; }
        int len = l.size();
        int i = idx < 0 ? len + idx : idx;
        if (i < 0 || i >= len) { c.replyNil(); return; }
        c.replyBulk(l.get(i));
    }

    public static void lrange(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer start = Args.parseInt(c, 2);
        Integer stop = Args.parseInt(c, 3);
        if (start == null || stop == null) return;
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null || l.isEmpty()) { c.replyEmptyArray(); return; }
        int len = l.size();
        int s = start < 0 ? len + start : start;
        int e = stop < 0 ? len + stop : stop;
        if (s < 0) s = 0;
        if (e >= len) e = len - 1;
        if (s > e) { c.replyEmptyArray(); return; }
        List<Object> out = new java.util.ArrayList<>(e - s + 1);
        for (int i = s; i <= e; i++) out.add(l.get(i));
        c.replyArray(out);
    }

    public static void lset(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer idx = Args.parseInt(c, 2);
        if (idx == null) return;
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyError("ERR no such key"); return; }
        int i = idx < 0 ? l.size() + idx : idx;
        if (i < 0 || i >= l.size()) { c.replyError("ERR index out of range"); return; }
        l.set(i, c.arg(3));
        Values.saveList(c.storage(), c.arg(1), l, null);
        c.replyOK();
    }

    public static void lrem(RedisClient c) {
        // LREM key count element
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer count = Args.parseInt(c, 2);
        if (count == null) return;
        String elem = c.arg(3);
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyInteger(0); return; }
        long removed = 0;
        int abs = Math.abs(count);
        if (count >= 0) {
            for (java.util.ListIterator<String> it = l.listIterator(); it.hasNext(); ) {
                if (elem.equals(it.next())) {
                    it.remove();
                    removed++;
                    if (count > 0 && removed >= abs) break;
                }
            }
        } else {
            // tail-to-head
            java.util.ListIterator<String> it = l.listIterator(l.size());
            while (it.hasPrevious()) {
                if (elem.equals(it.previous())) {
                    it.remove();
                    removed++;
                    if (removed >= abs) break;
                }
            }
        }
        Values.saveList(c.storage(), c.arg(1), l, null);
        c.replyInteger(removed);
    }

    public static void ltrim(RedisClient c) {
        if (c.argc() != 4) { c.replyWrongArgc(); return; }
        Integer start = Args.parseInt(c, 2);
        Integer stop = Args.parseInt(c, 3);
        if (start == null || stop == null) return;
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyOK(); return; }
        int len = l.size();
        int s = start < 0 ? len + start : start;
        int e = stop < 0 ? len + stop : stop;
        if (s < 0) s = 0;
        if (e >= len) e = len - 1;
        if (s > e || s >= len) {
            l.clear();
            c.storage().delete(c.arg(1));
        } else {
            LinkedList<String> kept = new LinkedList<>(l.subList(s, e + 1));
            Values.saveList(c.storage(), c.arg(1), kept, null);
        }
        c.replyOK();
    }

    public static void linsert(RedisClient c) {
        // LINSERT key BEFORE|AFTER pivot element
        if (c.argc() != 5) { c.replyWrongArgc(); return; }
        String where = c.arg(2).toUpperCase();
        String pivot = c.arg(3), elem = c.arg(4);
        if (!where.equals("BEFORE") && !where.equals("AFTER")) {
            c.replyError("ERR syntax error"); return;
        }
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), c.arg(1));
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null) { c.replyInteger(0); return; }
        int idx = l.indexOf(pivot);
        if (idx < 0) { c.replyInteger(-1); return; }
        int insertAt = where.equals("BEFORE") ? idx : idx + 1;
        l.add(insertAt, elem);
        Values.saveList(c.storage(), c.arg(1), l, null);
        c.replyInteger(l.size());
    }

    public static void rpoplpush(RedisClient c) {
        if (c.argc() != 3) { c.replyWrongArgc(); return; }
        moveOne(c, c.arg(1), c.arg(2));
    }

    public static void lmove(RedisClient c) {
        // LMOVE src dst LEFT|RIGHT LEFT|RIGHT
        if (c.argc() != 5) { c.replyWrongArgc(); return; }
        String srcSide = c.arg(3).toUpperCase();
        String dstSide = c.arg(4).toUpperCase();
        if (!srcSide.equals("LEFT") && !srcSide.equals("RIGHT")) {
            c.replyError("ERR syntax error"); return;
        }
        if (!dstSide.equals("LEFT") && !dstSide.equals("RIGHT")) {
            c.replyError("ERR syntax error"); return;
        }
        // Only implement the canonical RPOPLPUSH (RIGHT from src, LEFT to dst) for now.
        if (srcSide.equals("RIGHT") && dstSide.equals("LEFT")) {
            moveOne(c, c.arg(1), c.arg(2));
        } else {
            // Generic but slower path for the other 3 combinations.
            genericLMove(c, c.arg(1), c.arg(2), srcSide, dstSide);
        }
    }

    private static void moveOne(RedisClient c, String src, String dst) {
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), src);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null || l.isEmpty()) { c.replyNil(); return; }
        String v = l.removeLast();
        Values.saveList(c.storage(), src, l, null);

        LinkedList<String> dstList;
        try {
            dstList = Values.getOrCreateList(c.storage(), dst);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        dstList.addFirst(v);
        Values.saveList(c.storage(), dst, dstList, null);
        c.replyBulk(v);
    }

    private static void genericLMove(RedisClient c, String src, String dst,
                                     String srcSide, String dstSide) {
        LinkedList<String> l;
        try {
            l = Values.getList(c.storage(), src);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (l == null || l.isEmpty()) { c.replyNil(); return; }
        String v = srcSide.equals("LEFT") ? l.removeFirst() : l.removeLast();
        Values.saveList(c.storage(), src, l, null);

        LinkedList<String> dstList;
        try {
            dstList = Values.getOrCreateList(c.storage(), dst);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }
        if (dstSide.equals("LEFT")) dstList.addFirst(v);
        else                       dstList.addLast(v);
        Values.saveList(c.storage(), dst, dstList, null);
        c.replyBulk(v);
    }
}
