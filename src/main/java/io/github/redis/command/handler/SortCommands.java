package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;
import io.github.redis.storage.ValueType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * SORT command — full implementation supporting BY, GET, LIMIT, ASC/DESC, ALPHA,
 * STORE. Covers most production use cases.
 */
public final class SortCommands {

    private SortCommands() {}

    public static void sort(RedisClient c) {
        // SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE dst]
        if (c.argc() < 2) { c.replyWrongArgc(); return; }
        String key = c.arg(1);

        String byPattern = null;
        long offset = 0, count = -1;
        List<String> getPatterns = new ArrayList<>();
        boolean desc = false;
        boolean alpha = false;
        String store = null;

        for (int j = 2; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "BY":     if (++j >= c.argc()) { c.replySyntaxError(); return; } byPattern = c.arg(j); break;
                case "LIMIT":  if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
                    Long o = Args.parseLong(c, j + 1);
                    Long n = Args.parseLong(c, j + 2);
                    if (o == null || n == null) return;
                    offset = o; count = n; j += 2; break;
                case "GET":    if (++j >= c.argc()) { c.replySyntaxError(); return; } getPatterns.add(c.arg(j)); break;
                case "ASC":    desc = false; break;
                case "DESC":   desc = true;  break;
                case "ALPHA":  alpha = true;  break;
                case "STORE":  if (++j >= c.argc()) { c.replySyntaxError(); return; } store = c.arg(j); break;
                default:       c.replySyntaxError(); return;
            }
        }

        // Gather source items.
        List<String> items = new ArrayList<>();
        ValueType t = Values.typeOf(c.storage(), key);
        if (t == ValueType.LIST) {
            java.util.LinkedList<String> l = Values.getList(c.storage(), key);
            if (l != null) items.addAll(l);
        } else if (t == ValueType.SET) {
            Object carrier = Values.getSet(c.storage(), key);
            if (carrier != null) items.addAll(Values.setAsList(carrier));
        } else if (t == ValueType.SORTED_SET) {
            TreeMap<String, Double> z = Values.getZSet(c.storage(), key);
            if (z != null) items.addAll(z.keySet());
        } else if (t == null) {
            // empty key — return empty
        } else {
            c.replyError("WRONGTYPE Operation against a key holding the wrong kind of value");
            return;
        }

        // Compute sort key per item.
        List<Item> resolved = new ArrayList<>(items.size());
        for (String item : items) {
            String weight = byPattern == null ? item : expandPattern(byPattern, item);
            if (weight != null && byPattern != null) {
                String v = null;
                try { v = Values.getString(c.storage(), weight); } catch (Values.WrongTypeException ignored) {}
                weight = v;
            }
            resolved.add(new Item(item, weight));
        }

        // Sort.
        Comparator<Item> cmp;
        if (alpha) {
            cmp = Comparator.comparing(i -> i.weight == null ? "" : i.weight);
        } else {
            cmp = (a, b) -> {
                double da = toDouble(a.weight);
                double db = toDouble(b.weight);
                return Double.compare(da, db);
            };
        }
        if (desc) cmp = cmp.reversed();
        try {
            resolved.sort(cmp);
        } catch (RuntimeException e) {
            c.replyError("ERR One or more scores can't be converted into double");
            return;
        }

        // Apply LIMIT.
        if (offset > 0 || count >= 0) {
            int from = (int) Math.min(offset, resolved.size());
            int to = (count < 0) ? resolved.size() : (int) Math.min(resolved.size(), from + count);
            resolved = new ArrayList<>(resolved.subList(from, to));
        }

        // Apply GET (no GET → emit items themselves).
        List<String> out = new ArrayList<>();
        List<String> patterns = getPatterns.isEmpty() ? new ArrayList<>(java.util.Collections.singletonList("#")) : getPatterns;
        for (Item it : resolved) {
            for (String pat : patterns) {
                if (pat.equals("#")) {
                    // "#" → the item itself, no lookup needed.
                    out.add(it.value);
                    continue;
                }
                String lookup = expandPattern(pat, it.value);
                if (lookup == null) { out.add(null); continue; }
                String v = null;
                try { v = Values.getString(c.storage(), lookup); } catch (Values.WrongTypeException wte) {
                    v = null;
                }
                out.add(v);
            }
        }

        if (store != null) {
            // STORE: replace dst with a list of the (stringified) results.
            if (out.isEmpty()) {
                c.storage().delete(store);
            } else {
                java.util.LinkedList<String> list = new java.util.LinkedList<>();
                for (String s : out) list.addLast(s == null ? "" : s);
                Values.saveList(c.storage(), store, list, null);
            }
            c.replyInteger(out.size());
        } else {
            c.replyArray(new ArrayList<>(out));
        }
    }

    /** Replace the first '*' in {@code pattern} with {@code item}. */
    private static String expandPattern(String pattern, String item) {
        int idx = pattern.indexOf('*');
        if (idx < 0) return pattern;
        return pattern.substring(0, idx) + item + pattern.substring(idx + 1);
    }

    private static double toDouble(String s) {
        if (s == null) return 0.0;
        try { return Double.parseDouble(s); }
        catch (NumberFormatException e) {
            throw new RuntimeException("not a double");
        }
    }

    private static final class Item {
        final String value;
        final String weight;
        Item(String v, String w) { value = v; weight = w; }
    }
}
