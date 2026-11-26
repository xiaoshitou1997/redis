package io.github.redis.command.handler;

import io.github.redis.core.RedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * GEO commands. Stores coordinates as a ZSET where the score encodes
 * (longitude, latitude) packed into a 52-bit integer; member names are the
 * geo points.
 *
 * <p>Distance math uses the Haversine formula on a spherical earth model
 * (mean Earth radius = 6372797.560856 m).
 *
 * <p>Implemented: GEOADD / GEODIST / GEOHASH / GEOPOS / GEOSEARCH /
 * GEOSEARCHSTORE (with BYRADIUS / BYBOX, ASC/DESC, COUNT, WITHCOORD,
 * WITHDIST, WITHHASH).
 */
public final class GeoCommands {

    private GeoCommands() {}

    private static final double EARTH_RADIUS_M = 6372797.560856;
    private static final double GEO_LAT_MIN = -85.05112878;
    private static final double GEO_LAT_MAX =  85.05112878;
    private static final double GEO_LONG_MIN = -180.0;
    private static final double GEO_LONG_MAX =  180.0;

    // ===== Encoding / decoding =====

    /** Encode (longitude, latitude) into a 52-bit score. */
    private static double encodeGeohash(double lon, double lat) {
        if (lon < GEO_LONG_MIN || lon > GEO_LONG_MAX) return Double.NaN;
        if (lat < GEO_LAT_MIN  || lat > GEO_LAT_MAX)  return Double.NaN;
        double minLat = GEO_LAT_MIN, maxLat = GEO_LAT_MAX;
        double minLon = GEO_LONG_MIN, maxLon = GEO_LONG_MAX;
        long result = 0;
        // GeoHash interleaves longitude and latitude bits.
        // We use the standard 26/26 split (52 bits total).
        for (int i = 0; i < 26; i++) {
            double midLon = (minLon + maxLon) / 2;
            if (lon >= midLon) {
                result |= 1L << (51 - (i * 2));
                minLon = midLon;
            } else {
                maxLon = midLon;
            }
            double midLat = (minLat + maxLat) / 2;
            if (lat >= midLat) {
                result |= 1L << (50 - (i * 2));
                minLat = midLat;
            } else {
                maxLat = midLat;
            }
        }
        return result;
    }

    /** Decode a score back into (longitude, latitude). */
    private static double[] decodeGeohash(double score) {
        double minLat = GEO_LAT_MIN, maxLat = GEO_LAT_MAX;
        double minLon = GEO_LONG_MIN, maxLon = GEO_LONG_MAX;
        long bits = (long) score;
        for (int i = 0; i < 26; i++) {
            long lonBit = (bits >>> (51 - i * 2)) & 1;
            double midLon = (minLon + maxLon) / 2;
            if (lonBit == 1) minLon = midLon; else maxLon = midLon;
            long latBit = (bits >>> (50 - i * 2)) & 1;
            double midLat = (minLat + maxLat) / 2;
            if (latBit == 1) minLat = midLat; else maxLat = midLat;
        }
        return new double[] { (minLon + maxLon) / 2, (minLat + maxLat) / 2 };
    }

    /** Haversine distance in meters between two (lon, lat) pairs. */
    private static double distanceMeters(double lon1, double lat1, double lon2, double lat2) {
        double lat1r = Math.toRadians(lat1);
        double lat2r = Math.toRadians(lat2);
        double dlat = Math.toRadians(lat2 - lat1);
        double dlon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dlat / 2) * Math.sin(dlat / 2)
                + Math.cos(lat1r) * Math.cos(lat2r) * Math.sin(dlon / 2) * Math.sin(dlon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_M * c;
    }

    private static String formatDist(double meters, String unit) {
        double v = convertFromMeters(meters, unit);
        return StringCommands.formatFloat(v);
    }

    private static double convertToMeters(double v, String unit) {
        if (unit == null) return v;
        switch (unit.toLowerCase()) {
            case "m": return v;
            case "km": return v * 1000;
            case "mi": return v * 1609.34;
            case "ft": return v * 0.3048;
            default: return Double.NaN;
        }
    }

    private static double convertFromMeters(double meters, String unit) {
        if (unit == null) return meters;
        switch (unit.toLowerCase()) {
            case "m": return meters;
            case "km": return meters / 1000;
            case "mi": return meters / 1609.34;
            case "ft": return meters / 0.3048;
            default: return Double.NaN;
        }
    }

    // ===== GEOADD =====

    public static void geoadd(RedisClient c) {
        // GEOADD key [NX|XX] [CH] longitude latitude member [lon lat member ...]
        if (c.argc() < 5) { c.replyWrongArgc(); return; }
        int j = 1;
        String key = c.arg(j++);
        boolean nx = false, xx = false, ch = false;
        flagParse:
        for (; j < c.argc(); j++) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "NX": nx = true; break;
                case "XX": xx = true; break;
                case "CH": ch = true; break;
                default:
                    break flagParse;
            }
        }
        if ((c.argc() - j) % 3 != 0 || c.argc() == j) { c.replySyntaxError(); return; }

        // Build a ZADD-style operation on the underlying zset, using our encoded score.
        TreeMap<String, Double> z;
        try {
            z = Values.getOrCreateZSet(c.storage(), key);
        } catch (Values.WrongTypeException wte) {
            c.replyError(wte.getMessage()); return;
        }

        long added = 0, changed = 0;
        for (; j + 2 < c.argc(); j += 3) {
            Double lon = Args.parseDouble(c, j);
            Double lat = Args.parseDouble(c, j + 1);
            String member = c.arg(j + 2);
            if (lon == null || lat == null) return;
            if (lon < GEO_LONG_MIN || lon > GEO_LONG_MAX
                    || lat < GEO_LAT_MIN || lat > GEO_LAT_MAX) {
                c.replyError("ERR invalid longitude,latitude pair " + lon + "," + lat); return;
            }
            double score = encodeGeohash(lon, lat);
            Double prev = z.get(member);
            if (prev == null) {
                if (xx) continue;
                z.put(member, score);
                added++;
            } else if (prev != score) {
                if (nx) continue;
                z.put(member, score);
                changed++;
            }
        }
        Values.saveZSet(c.storage(), key, z, null);
        c.replyInteger(ch ? added + changed : added);
    }

    // ===== GEODIST =====

    public static void geodist(RedisClient c) {
        // GEODIST key member1 member2 [m|km|mi|ft]
        if (c.argc() < 4 || c.argc() > 5) { c.replyWrongArgc(); return; }
        String unit = c.argc() == 5 ? c.arg(4) : "m";
        TreeMap<String, Double> z;
        try { z = Values.getZSet(c.storage(), c.arg(1)); }
        catch (Values.WrongTypeException wte) { c.replyError(wte.getMessage()); return; }
        if (z == null) { c.replyNil(); return; }
        Double s1 = z.get(c.arg(2));
        Double s2 = z.get(c.arg(3));
        if (s1 == null || s2 == null) { c.replyNil(); return; }
        double[] p1 = decodeGeohash(s1);
        double[] p2 = decodeGeohash(s2);
        double m = distanceMeters(p1[0], p1[1], p2[0], p2[1]);
        c.replyBulk(formatDist(m, unit));
    }

    // ===== GEOHASH =====

    public static void geohash(RedisClient c) {
        // GEOHASH key member [member ...]
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try { z = Values.getZSet(c.storage(), c.arg(1)); }
        catch (Values.WrongTypeException wte) { c.replyError(wte.getMessage()); return; }
        List<Object> out = new ArrayList<>(c.argc() - 2);
        for (int j = 2; j < c.argc(); j++) {
            if (z == null) { out.add(null); continue; }
            Double s = z.get(c.arg(j));
            if (s == null) { out.add(null); continue; }
            double[] p = decodeGeohash(s);
            out.add(geohashString(p[0], p[1]));
        }
        c.replyArray(out);
    }

    private static final String GEOHASH_ALPHABET = "0123456789bcdefghjkmnpqrstuvwxyz";

    /** Standard 11-character base32 geohash. */
    private static String geohashString(double lon, double lat) {
        // Re-encode as interleaved 5-bit groups → base32.
        double minLat = GEO_LAT_MIN, maxLat = GEO_LAT_MAX;
        double minLon = GEO_LONG_MIN, maxLon = GEO_LONG_MAX;
        boolean evenBit = true;
        int bit = 0, ch = 0;
        StringBuilder sb = new StringBuilder(11);
        while (sb.length() < 11) {
            if (evenBit) {
                double mid = (minLon + maxLon) / 2;
                if (lon >= mid) { ch |= (1 << (4 - bit)); minLon = mid; } else { maxLon = mid; }
            } else {
                double mid = (minLat + maxLat) / 2;
                if (lat >= mid) { ch |= (1 << (4 - bit)); minLat = mid; } else { maxLat = mid; }
            }
            evenBit = !evenBit;
            if (bit < 4) {
                bit++;
            } else {
                sb.append(GEOHASH_ALPHABET.charAt(ch));
                bit = 0; ch = 0;
            }
        }
        return sb.toString();
    }

    // ===== GEOPOS =====

    public static void geopos(RedisClient c) {
        if (c.argc() < 3) { c.replyWrongArgc(); return; }
        TreeMap<String, Double> z;
        try { z = Values.getZSet(c.storage(), c.arg(1)); }
        catch (Values.WrongTypeException wte) { c.replyError(wte.getMessage()); return; }
        List<Object> out = new ArrayList<>(c.argc() - 2);
        for (int j = 2; j < c.argc(); j++) {
            if (z == null) { out.add(null); continue; }
            Double s = z.get(c.arg(j));
            if (s == null) { out.add(null); continue; }
            double[] p = decodeGeohash(s);
            List<Object> lonLat = new ArrayList<>(2);
            lonLat.add(StringCommands.formatFloat(p[0]));
            lonLat.add(StringCommands.formatFloat(p[1]));
            out.add(lonLat);
        }
        c.replyArray(out);
    }

    // ===== GEOSEARCH / GEOSEARCHSTORE =====

    public static void geosearch(RedisClient c) { geosearchImpl(c, false); }
    public static void geosearchstore(RedisClient c) { geosearchImpl(c, true); }

    @SuppressWarnings("unchecked")
    private static void geosearchImpl(RedisClient c, boolean store) {
        // GEOSEARCH key <FROMMEMBER member | FROMLONLAT lon lat>
        //   <BYRADIUS radius m|km|mi|ft | BYBOX width height m|km|mi|ft>
        //   [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
        // GEOSEARCHSTORE dest src ... (same options) [STOREDIST]
        int j = 1;
        String srcKey, destKey = null;
        if (store) {
            if (c.argc() < 3) { c.replyWrongArgc(); return; }
            destKey = c.arg(1);
            srcKey = c.arg(2);
            j = 3;
        } else {
            if (c.argc() < 2) { c.replyWrongArgc(); return; }
            srcKey = c.arg(1);
            j = 2;
        }
        boolean storedist = false;

        // Parse FROM
        if (j >= c.argc()) { c.replySyntaxError(); return; }
        String fromOpt = c.arg(j).toUpperCase();
        double centerLon = 0, centerLat = 0;
        TreeMap<String, Double> srcZ;
        try { srcZ = Values.getZSet(c.storage(), srcKey); }
        catch (Values.WrongTypeException wte) { c.replyError(wte.getMessage()); return; }
        if (fromOpt.equals("FROMMEMBER")) {
            if (j + 1 >= c.argc()) { c.replySyntaxError(); return; }
            Double s = (srcZ == null) ? null : srcZ.get(c.arg(j + 1));
            if (s == null) { c.replyError("ERR could not decode requested zset member"); return; }
            double[] p = decodeGeohash(s);
            centerLon = p[0]; centerLat = p[1];
            j += 2;
        } else if (fromOpt.equals("FROMLONLAT")) {
            if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
            Double lon = Args.parseDouble(c, j + 1);
            Double lat = Args.parseDouble(c, j + 2);
            if (lon == null || lat == null) return;
            centerLon = lon; centerLat = lat;
            j += 3;
        } else {
            c.replySyntaxError(); return;
        }

        // Parse BY
        if (j >= c.argc()) { c.replySyntaxError(); return; }
        String byOpt = c.arg(j).toUpperCase();
        double radiusM = 0, widthM = 0, heightM = 0;
        boolean byRadius = false, byBox = false;
        if (byOpt.equals("BYRADIUS")) {
            if (j + 2 >= c.argc()) { c.replySyntaxError(); return; }
            Double r = Args.parseDouble(c, j + 1);
            if (r == null) return;
            radiusM = convertToMeters(r, c.arg(j + 2));
            byRadius = true;
            j += 3;
        } else if (byOpt.equals("BYBOX")) {
            if (j + 3 >= c.argc()) { c.replySyntaxError(); return; }
            Double w = Args.parseDouble(c, j + 1);
            Double h = Args.parseDouble(c, j + 2);
            if (w == null || h == null) return;
            widthM = convertToMeters(w, c.arg(j + 3));
            heightM = convertToMeters(h, c.arg(j + 3));
            byBox = true;
            j += 4;
        } else {
            c.replySyntaxError(); return;
        }

        boolean asc = true, explicitDir = false;
        int count = -1;
        boolean withcoord = false, withdist = false, withhash = false;
        while (j < c.argc()) {
            String opt = c.arg(j).toUpperCase();
            switch (opt) {
                case "ASC":  asc = true; explicitDir = true; j++; break;
                case "DESC": asc = false; explicitDir = true; j++; break;
                case "COUNT":
                    if (j + 1 >= c.argc()) { c.replySyntaxError(); return; }
                    Integer n = Args.parseInt(c, j + 1);
                    if (n == null) return;
                    count = n;
                    j += 2;
                    if (j < c.argc() && c.arg(j).equalsIgnoreCase("ANY")) j++;
                    break;
                case "WITHCOORD": withcoord = true; j++; break;
                case "WITHDIST":  withdist = true; j++; break;
                case "WITHHASH":  withhash = true; j++; break;
                case "STOREDIST": storedist = true; j++; break; // GEOSEARCHSTORE only
                default: c.replySyntaxError(); return;
            }
        }

        // Collect candidates from srcZ that match.
        if (srcZ == null) {
            if (store) c.replyInteger(0); else c.replyEmptyArray();
            return;
        }
        List<double[]> hits = new ArrayList<>(); // [member-hash, dist, lon, lat]
        List<String> members = new ArrayList<>();
        for (Map.Entry<String, Double> e : srcZ.entrySet()) {
            double[] p = decodeGeohash(e.getValue());
            double d = distanceMeters(centerLon, centerLat, p[0], p[1]);
            boolean inside;
            if (byRadius) {
                inside = d <= radiusM;
            } else {
                // Box: approximate using width/height as planar distances.
                double dLon = distanceMeters(centerLon, centerLat, p[0], centerLat);
                double dLat = distanceMeters(centerLon, centerLat, centerLon, p[1]);
                inside = dLon <= widthM / 2 && dLat <= heightM / 2;
            }
            if (inside) {
                hits.add(new double[] { e.getValue(), d, p[0], p[1] });
                members.add(e.getKey());
            }
        }
        // Sort by distance, ASC or DESC.
        if (asc) {
            hits.sort((a, b) -> Double.compare(a[1], b[1]));
        } else {
            hits.sort((a, b) -> Double.compare(b[1], a[1]));
        }

        // Apply count.
        if (count >= 0 && hits.size() > count) {
            hits = new ArrayList<>(hits.subList(0, count));
            members = new ArrayList<>(members.subList(0, count));
        }

        if (store) {
            // GEOSEARCHSTORE dest: write members (or dists) as a new zset.
            TreeMap<String, Double> out = new TreeMap<>();
            for (int i = 0; i < hits.size(); i++) {
                double v = storedist ? hits.get(i)[1] : hits.get(i)[0];
                out.put(members.get(i), v);
            }
            if (out.isEmpty()) c.storage().delete(destKey);
            else               Values.saveZSet(c.storage(), destKey, out, null);
            c.replyInteger(out.size());
            return;
        }

        // Reply array.
        List<Object> replyRows = new ArrayList<>(hits.size());
        for (int i = 0; i < hits.size(); i++) {
            if (!withcoord && !withdist && !withhash) {
                replyRows.add(members.get(i));
                continue;
            }
            List<Object> row = new ArrayList<>();
            row.add(members.get(i));
            if (withdist)  row.add(StringCommands.formatFloat(hits.get(i)[1]));
            if (withhash)  row.add((long) Double.doubleToLongBits(hits.get(i)[0]));
            if (withcoord) {
                List<Object> ll = new ArrayList<>();
                ll.add(StringCommands.formatFloat(hits.get(i)[2]));
                ll.add(StringCommands.formatFloat(hits.get(i)[3]));
                row.add(ll);
            }
            replyRows.add(row);
        }
        c.replyArray(replyRows);
    }
}
