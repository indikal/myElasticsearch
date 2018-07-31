package lk.indika.elasticsearch;

import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

public class StringUtils
{
    /**
     * An empty string.
     */
    public static final String EMPTY_STRING = "";

    /**
     * Line feed character.
     */
    public static final char LINE_FEED = '\r';    // (aka u000A);

    /**
     * A String 0.
     */
    private static final String STRING_ZERO = "0";

    /**
     * The default length for a chomp()
     */
    private static int chompLength = 32;

    public static int getChompLength()
    {
        return chompLength;
    }

    public static void setChompLength(int chompLength)
    {
        StringUtils.chompLength = chompLength;
    }

    /**
     * Returns true if null or trims to an empty string.
     *
     * @param s ...
     * @return boolean ...
     * @deprecated Use #isEmpty() or #isNotEmpty() instead
     */
    public static boolean isBlank(String s)
    {
        return ((null == s) || (EMPTY_STRING.equals(s.trim())));
    }

    /**
     * Returns true if null or zero.
     *
     * @param key ...
     * @return boolean ...
     * @deprecated Use #isEmpty() or #isNotEmpty() instead
     */
    public static boolean isBlank(Integer key)
    {
        return ((null == key) || (0 == key.intValue()));
    }

    /**
     * Returns true if null or trims to an empty string.
     *
     * @param s ...
     * @return boolean ...
     * @deprecated Use #isEmpty() or #isNotEmpty() instead
     */
    public static boolean isBlankValue(String s)
    {
        if (null == s) {
            return true;
        }
        String str = s.trim();
        return ((EMPTY_STRING.equals(str)) || (STRING_ZERO.equals(str)));
    }

    /**
     * Returns null or a trimmed uppercase string.
     *
     * @param string ...
     * @return String ...
     */
    public static String toUpperOrNull(String string)
    {
        if (null != string) {
            return string.toUpperCase().trim();
        }
        return null;
    }

    /**
     * Returns the count left characters from string str
     *
     * @param str   ...
     * @param count ...
     * @return ...
     */
    public static String left(String str, int count)
    {
        if ((str != null) && (str.length() > count)) {
            return str.substring(0, count);
        }
        return str;
    }

    /**
     * Chomp the string to count characters long with "..." at the end
     * to indicate that a chomp was made. Use whatever property chompLength is set to
     *
     * @param str ...
     * @return ...
     */
    public static String chomp(String str)
    {
        return chomp(str, chompLength);
    }

    /**
     * Chomp the string to count characters long with "..." at the end
     * to indicate that a chomp was made.
     *
     * @param str   ...
     * @param count ...
     * @return ...
     */
    public static String chomp(String str, int count)
    {
        if ((str != null) && (count > 3) && (str.length() > (count - 3))) {
            return left(str, count - 3) + "...";
        }
        else {
            return str;
        }
    }

    /**
     * Add a specific character until the String is a specific length
     *
     * @param str The original string, if this String object is null this method will return null
     * @param ch  The character to add at the end of the string
     * @param len The length the string should be
     * @return A new string with a number of characters appended at the end so it gets the specified length
     */
    public static String addCharUntilLength(String str, char ch, long len)
    {
        if (str != null) {
            StringBuffer sb = new StringBuffer(str);
            for (int i = str.length(); i < len; i++) {
                sb.append(ch);
            }
            return sb.toString();
        }
        else {
            return null;
        }
    }

    /**
     * Inserts a specific character in the beginning of string until the String is a specific length
     *
     * @param str The original string, if this String object is null this method will return null
     * @param ch  The character to insert at the beginning of the string
     * @param len The length the string should be
     * @return A new string with a number of characters inserted at the beginning so it gets the specified length
     */
    public static String insertCharUntilLength(String str, char ch, long len)
    {
        if (str != null) {
            StringBuffer sb = new StringBuffer(str);
            for (int i = str.length(); i < len; i++) {
                sb.insert(0, ch);
            }
            return sb.toString();
        }
        else {
            return null;
        }
    }

    /**
     * If the specified postfix exists at the end of the string shorten the string at the
     * end with the length of the postfix. It will try to remove the postfix until it no longer
     * matches the end of the string.
     *
     * @param str     The string to shorten
     * @param postfix The string will be shorten as long as the string end matches this prefix
     * @return The shorten string
     */
    public static String removeFromEnd(String str, String postfix)
    {
        if (str != null) {
            StringBuffer sb = new StringBuffer(str);
            while (sb.toString().endsWith(postfix)) {
                sb.setLength(sb.length() - postfix.length());
            }
            return sb.toString();
        }
        else {
            return null;
        }
    }

    public static String toString(Object obj)
    {
        return toString(obj, "");
    }

    public static String toString(Object obj, String indent)
    {
        StringBuffer sb = new StringBuffer();
        // Check for null values
        if (obj == null) {
            return "{null}";
        }
        Iterator iterator = null;
        // If parameter is some kind of Collection, construct an iterator for it. Otherwise do nothing
        if (obj.getClass().isArray()) {
            try {
                // If we're lucky, it is an array of objects
                // that we can iterate over with no copying
                iterator = Arrays.asList((Object[]) obj).iterator();
            }
            catch (ClassCastException e) {
                // Rats -- It is an array of primitives
                int length = Array.getLength(obj);
                ArrayList c = new ArrayList(length);
                for (int i = 0; i < length; i++) {
                    c.add(Array.get(obj, i));
                }
                iterator = c.iterator();
            }
        }
        else if (obj instanceof Collection) {
            iterator = ((Collection) obj).iterator();
        }
        else if (obj instanceof Iterator) {
            iterator = (Iterator) obj;
        }
        else if (obj instanceof Map) {
            iterator = ((Map) obj).entrySet().iterator();
        }
        else if (obj instanceof Enumeration) {
            List list = new ArrayList();
            for (Enumeration e = (Enumeration) obj; e.hasMoreElements();) {
                list.add(e.nextElement());
            }
            iterator = list.iterator();
        }
        // Take care of either created iterator or use the parameter itself
        if (iterator != null) {
            int i = 0;
            sb.append("{\r\n");
            while (iterator.hasNext()) {
                Object o = iterator.next();
                sb.append(indent).append("[").append(i).append("]=");
                // Take care of string handling recursive
                sb.append(StringUtils.toString(o, indent + "  "));
                i++;
            }
            sb.append(indent).append("}\r\n");
        }
        else {
            if (obj instanceof Date) {
                //sb.append(DateUtil.format((Date) obj, "yyyy-MM-dd HH:mm:ss.SSS"));
            }
            else if (obj.getClass().getName().startsWith("java.lang")) {
                sb.append(obj.toString());
            }
            else {
                sb.append("{\r\n");
                sb.append(StringUtils.reflect(obj, indent));
                sb.append(indent).append("}\r\n");
            }
        }
        // Return created value
        return sb.toString();
    }

    private static String reflect(Object object, String indent)
    {
        StringBuffer sb = new StringBuffer();
        Method[] m = object.getClass().getMethods();
        for (int i = 0; i < m.length; i++) {
            Method method = m[i];
            // Find and invoke public methods starting with "get" or "is"
            if (Modifier.isPublic(method.getModifiers()) && (method.getName().startsWith("get") || method.getName().startsWith("is"))) {
                if (!"getClass".equals(method.getName())) {
                    try {
                        Object value = method.invoke(object, new Object[0]);
                        String stringValue = String.valueOf(value);
                        if (method.getName().startsWith("get")) {
                            if (value == null) {
                                //sb.append(indent + method.getName().substring(3) + "={null}\r\n");
                            }
                            else {
                                if (value instanceof Date) {
                                    //sb.append(indent).append(method.getName().substring(3)).append("=").append(DateUtil.format((Date) value, "yyyy-MM-dd HH:mm:ss.SSS")).append("\r\n");
                                }
                                else if (value.getClass().getName().startsWith("java.lang")) {
                                    sb.append(indent).append(method.getName().substring(3)).append("=").append(stringValue).append("\r\n");
                                }
                                else {
                                    // Call for recursive behaviour
                                    sb.append(indent).append(method.getName().substring(3)).append("=").append(StringUtils.toString(value, indent + "  ")).append("\r\n");
                                }
                            }
                        }
                        if (method.getName().startsWith("is")) {
                            if (value == null) {
                                //sb.append(indent + method.getName().substring(2) + "={null}\r\n");
                            }
                            else {
                                if (value.getClass().getName().startsWith("java.lang")) {
                                    sb.append(indent).append(method.getName().substring(2)).append("=").append(stringValue).append("\r\n");
                                }
                                else {
                                    // Call for recursive behaviour
                                    sb.append(indent).append(method.getName().substring(2)).append("=").append(StringUtils.toString(value, indent + "  ")).append("\r\n");
                                }
                            }
                        }
                    }
                    catch (Exception e) {
                        sb.append(indent).append("(Exception when reflecting ").append(method.getName()).append("! => ").append(e.getMessage()).append(")\r\n");
                    }
                }
            }
        }
        return sb.toString();
    }

    public static String normalizeCivicNo(String civicNo)
    {
        if (StringUtils.isNotEmpty(civicNo)) {
            if (civicNo.indexOf("-") == -1) {
                // Normalize civic reg no
                if (civicNo.length() == 10) {
                    // No century included, format is supposed to be YYMMDDXXXX
                    return civicNo.substring(0, 6) + "-" + civicNo.substring(6);
                }
                if (civicNo.length() == 12) {
                    // Century included, format is supposed to be YYYYMMDDXXXX
                    return civicNo.substring(0, 8) + "-" + civicNo.substring(8);
                }
            }
        }
        return civicNo;
    }

    public static boolean isEmpty(Object obj)
    {
        if(obj == null) {
            return true;
        }
        else if (obj instanceof String) {
            return ((String)obj).trim().length() <= 0;
        }
        else if (obj instanceof Date) {
            return ((Date)obj).getTime() <= 0;
        }
        else if (obj.getClass().isArray()) {
            return Array.getLength(obj) <= 0;
        }
        else if (obj instanceof Collection) {
            return ((Collection) obj).size() <= 0;
        }
        else if (obj instanceof Iterator) {
            return !((Iterator) obj).hasNext();
        }
        else if (obj instanceof Map) {
            return ((Map) obj).entrySet().size() <= 0;
        }
        else if (obj instanceof Enumeration) {
            return !((Enumeration) obj).hasMoreElements();
        }
        return false; // Object not null!
    }

    public static boolean isNotEmpty(Object obj)
    {
        return !isEmpty(obj);
    }

    /**
     * Returns a string with concatenated objects from an array. If a separator is supplied, that will be appended between every object. No separator is appended at the end of the String.
     * @param array An array of objects. The method makes use of the toString() method
     * @param separator A string containing what is dividing each element in the array, ex: ","
     * @return A concatenation of the objects in the array and possibly with separators (if not null or empty).
     */
    public static String getArrayAsAsString(Object[] array, String separator)
    {
        StringBuilder buf = new StringBuilder();
        for (int i = 0; i < array.length; i++)
        {
            Object o = array[i];
            buf.append(o);
            if (separator != null && separator.length() > 0 && i < (array.length - 1)) buf.append(separator);
        }
        return buf.toString();
    }

    /** HTML encodes the string, replaces special and reserved characters with html code equivalent
     *
     * @param pText
     * @return html encoded string
     */
    public static String encodeAsHTML(String pText) {
        StringTokenizer tokenizer =
                new StringTokenizer(pText, "\u00e5\u00c5\u00e4\u00c4\u00f6\u00d6\u00e9\u00c9&<>\"", true);
        int tokenCount = tokenizer.countTokens();
        if (tokenCount == 1)
            return pText;
        /* text.length + (tokenCount * 6) gives buffer large
        * enough so no addition memory would be needed and no
        * costly copy operations would occur  */
        StringBuffer buffer =
                new StringBuffer(pText.length() + tokenCount * 6);
        while (tokenizer.hasMoreTokens()) {
            String token = tokenizer.nextToken();
            if (token.length() == 1) {
                switch (token.charAt(0)) {
                    case '\u00E5':  //E5 0229 ?
                        buffer.append("&aring;");
                        break;
                    case '\u00C5': //C5 197 ?
                        buffer.append("&Aring;");
                        break;
                    case '\u00E4':  //E4 228 ?
                        buffer.append("&auml;");
                        break;
                    case '\u00C4':  //C4 196 ?
                        buffer.append("&Auml;");
                        break;
                    case '\u00F6': //F6 0246 ?
                        buffer.append("&ouml;");
                        break;
                    case '\u00D6': //D6 0214 ?
                        buffer.append("&Ouml;");
                        break;
                    case '\u00E9': //E9 0233  \A8\A6
                        buffer.append("&eacute;");
                        break;
                    case '\u00C9': //C9 0201 ?
                        buffer.append("&Eacute;");
                        break;
                    case '&':
                        buffer.append("&amp;");
                        break;
                    case '<':
                        buffer.append("&lt;");
                        break;
                    case '>':
                        buffer.append("&gt;");
                        break;
                    case '"':
                        buffer.append("&quot;");
                        break;
                    default:
                        buffer.append(token);
                }
            } else {
                buffer.append(token);
            }
        }
        return buffer.toString();
    }


    /** HTML decodes the string, replaces html code equivalent with the reserved characters
     *
     * @param pText
     * @return html decoded string
     * @throws UnsupportedEncodingException
     */
    public static String urlDecode(String pText) throws Exception {
        try {
            return URLDecoder.decode(pText, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Exception(e);
        }
    }

        /** if a string is actually a numeric
     *
     * @param str
     * @return boolean
     * @throws UnsupportedEncodingException
     */
    public   static   boolean   isNumber(String   str)   {
        //   +     -     digits   and   point   is   allowed
        return   java.util.regex.Pattern.matches("^[-+0-9.]*$",   str.trim());
    }

    /** if a string is actually a numeric, parse it
     *
     * @param str
     * @return boolean
     * @throws UnsupportedEncodingException
     */
    public   static   Integer getInteger(String   str)
    {
        if (isNumber( str))
            return Integer.parseInt(str);
        else
            return null;
    }

    /** HTML encodes the string, replaces the reserved characters with html code equivalent
     *
     * @param pText
     * @return html encoded string
     * @throws UnsupportedEncodingException
     */
    public static String urlEncode(String pText) throws Exception {
        try {
            return URLEncoder.encode(pText, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new Exception(e);
        }
    }

    public static void main(String[] args)
    {
        System.out.println("ARRAY AS STRING: " + StringUtils.getArrayAsAsString(new Long[]{Long.valueOf(123456), Long.valueOf(45647), Long.valueOf(7897413)}, ","));
        System.out.println("null: " + StringUtils.isEmpty(null));
        System.out.println("String(\"\"): " + StringUtils.isEmpty(""));
        System.out.println("String[0]: " + StringUtils.isEmpty(new String[0]));
        System.out.println("String[1]: " + StringUtils.isEmpty( new String[] {"a"} ));
        System.out.println("Date[0]: " + StringUtils.isEmpty(new Date(0)));
        System.out.println("Date[new date()]: " + StringUtils.isEmpty(new Date()));
        System.out.println("Collection[0]: " + StringUtils.isEmpty(new ArrayList()));
        System.out.println("Iterator[0]: " + StringUtils.isEmpty(new ArrayList().iterator()));
        System.out.println("Map[0]: " + StringUtils.isEmpty(new HashMap()));
        System.out.println("Enumeration[0]: " + StringUtils.isEmpty(new Hashtable().elements()));
    }

    public static String printBeanDetails(Object obj) throws Exception
    {        /*
           * Field[] fs=o.getClass().getDeclaredFields(); for(int
           * i=0;i<fs.length;i++) { System.out.println(fs[i].getName()+"="+fs[i]);
           * }
           */
        StringBuffer sb =new StringBuffer("");

         if (obj == null) {
            return "{null}";
        }
        Iterator iterator = null;
        // If parameter is some kind of Collection, construct an iterator for it. Otherwise do nothing
        if (obj.getClass().isArray()) {
            try {
                // If we're lucky, it is an array of objects
                // that we can iterate over with no copying
                iterator = Arrays.asList((Object[]) obj).iterator();
            }
            catch (ClassCastException e) {
                // Rats -- It is an array of primitives
                int length = Array.getLength(obj);
                ArrayList c = new ArrayList(length);
                for (int i = 0; i < length; i++) {
                    c.add(Array.get(obj, i));
                }
                iterator = c.iterator();
            }
        }
        else if (obj instanceof Collection) {
            iterator = ((Collection) obj).iterator();
        }
        else if (obj instanceof Iterator) {
            iterator = (Iterator) obj;
        }
        else if (obj instanceof Map) {
            iterator = ((Map) obj).entrySet().iterator();
        }
        else if (obj instanceof Enumeration) {
            List list = new ArrayList();
            for (Enumeration e = (Enumeration) obj; e.hasMoreElements();) {
                list.add(e.nextElement());
            }
            iterator = list.iterator();
        }
        // Take care of either created iterator or use the parameter itself
        if (iterator != null) {
            int i = 0;
            sb.append("{\r\n");
            while (iterator.hasNext()) {
                Object o = iterator.next();
                sb.append("\t").append("[").append(i).append("]=");
                // Take care of string handling recursive
                sb.append(printBeanDetails(o));
                i++;
            }
            sb.append("\t").append("}\r\n");
        }



        java.beans.BeanInfo bi = java.beans.Introspector.getBeanInfo(obj.getClass());
        java.beans.PropertyDescriptor[] pds = bi.getPropertyDescriptors();
        for (int i = 0; i < pds.length; i++) {
            try
            {
                sb=sb.append(""+pds[i].getName() + " := " + pds[i].getReadMethod().invoke(obj, new Object[] {})+"\n\r");
            } catch (Exception e) {
                // TODO Auto-generated catch block
                //e.printStackTrace();
            }
        }
        return sb.toString();
    }
}