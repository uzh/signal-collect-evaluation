//package com.signalcollect.visualization;
//
//import org.apache.commons.collections15.Transformer;
//
///**
// *
// * @param <V>
// * @author Philip Stutz
// */
//public class SmartToStringLabeller<V> implements Transformer<V, String> {
//
//    private Integer wordLimit;
//
//    /**
//     *
//     * Simple labeller class that converts strings to HTML, limits the length of
//     * Strings and adds " ..." to the end in case a string should
//     * be too long.
//     *
//     * @param wordLimit
//     */
//    public SmartToStringLabeller(Integer wordLimit) {
//        this.wordLimit = wordLimit;
//    }
//
//    /**
//     *
//     * @param argument
//     * @return
//     */
//    public String transform(V argument) {
//        String longString = argument.toString();
//        String[] tokens = longString.split(" ");
//        StringBuffer sb = new StringBuffer("<html>");
//        for (Integer i = 0; i < Math.min(tokens.length, wordLimit); i++) {
//            sb.append(tokens[i]);
//            if (tokens[i].endsWith(":")) {
//                sb.append("<br/>");
//            }
//            if (i < Math.min(tokens.length, wordLimit) - 1) {
//                sb.append(" ");
//            }
//        }
//        if (tokens.length > wordLimit) {
//            sb.append(" ...");
//        }
//        sb.append("</html>");
//        return sb.toString();
//    }
//
//}
