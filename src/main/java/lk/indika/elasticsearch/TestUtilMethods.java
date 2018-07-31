/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika.elasticsearch;

import java.util.regex.Pattern;
import org.apache.commons.lang.StringEscapeUtils;

/**
 *
 * @author indika
 */
public class TestUtilMethods {
    public static void main(String[] args) {
//        //assortment did
//        testRegExp("2433510:29320|2433511:29321|2433512:29244|2433513:29267,29322|123456:", 
//            "(^|.*\\|)(24335112|24335102|123456):.*");
//        
//        System.out.println();
//        //authorizer did
//        testRegExp("2433510:29320|2433511:29321|2433512:29244|2433513:29267,29322|123456:", 
//            "(^|.*\\|)(123456):.*");
//        
//        System.out.println();
//        System.out.println("delete assortment did ..."); //delete assortment did
//        replaceWithRegExp("2433510:29320|2433511:29321|2433510:29244|2433510:29267,29322|123456:", 
//            "2433510");
//        
//        System.out.println();
//        System.out.println("Check for assortment did");
//        testHasAssortment("2433510:|2433511:|2433510:|2433510:,29322|:");
//        
//        System.out.println();
//        System.out.println("Check for authorizer did");
//        testHasAuthorizer("2433510:29244|2433511:29321|2433512:29244|2433513:29322", 
//            ".*(:|\\,)(29322|24335).*");
        
//        removeAssortment("2433510:29320|2433511:29321|2433512:29244|2433513:29267,29322|2647045:26793|3186141:29322",
//                         "3186141");
//        removeAssortment("2433510:29320|2433511:29321|2433512:29244|2433513:29267,29322|2647045:26793|3186141:29322",
//                         "2433510");
//        removeAssortment("2433510:29320|2433511:29321|2433512:29244|2433513:29267,29322|2647045:26793|3186141:29322",
//                         "2433512");
        //testRegExp("30501-5772323", "^*577*$");
        //System.out.println(new String(" hello     there   ").trim().replaceAll(" +", " "));
        
        escapeSpecialRegexChars("h/äftklammer AND 24/6");
    }
    
    private static void testRegExp(String value, String regexp) {
        System.out.println("Value: " + value);
        regexp = regexp.replaceAll("\\*", ".*");
        System.out.println("Regexp: " + regexp);
        System.out.println("Match: " + value.matches(regexp));
    }
    
    private static void replaceWithRegExp(String value, String remove) {
        System.out.println("Value: " + value);
        System.out.println("Remove: " + remove);
        
        int begin = value.indexOf(remove);
        while (begin >= 0) {
            int end = value.indexOf("|", begin);
            end = (end < 0) ? value.length() : end+1;

            value = value.substring(0, begin) + value.substring(end);

            begin = value.indexOf(remove);
        }
        
        
//        if (begin > 0) {
//            int end = value.indexOf("|", begin);
//            end = (end < 0) ? value.length() : end;
//            
//            value = value.substring(0, begin-1) + value.substring(end);
//        }
        System.out.println("New value: " + value);
    }
    
    private static void testHasAssortment(String value) {
        String regexp = "(^?|.*\\|)\\d*:.*";
        System.out.println("Value: " + value);
        System.out.println("Regexp: " + regexp);
        System.out.println("Match: " + value.matches(regexp));
    }
    
    private static void testHasAuthorizer(String value, String regexp) {
        System.out.println("Value: " + value);
        System.out.println("Regexp: " + regexp);
        System.out.println("Match: " + value.matches(regexp));
    }
    
    private static void removeAssortment(String assortments, String assortmentDid) {
        System.out.println("-----------------------------------------------------------");
        System.out.println("All Assortments: " + assortments);
        System.out.println("Remove: " + assortmentDid);
        
        int begin = assortments.indexOf(assortmentDid);
        while (begin >= 0) {
            int end = assortments.indexOf("|", begin);
            begin = (begin > 0 && end < 0) ? begin-1 : begin;
            end = (end < 0) ? assortments.length() : end+1;

            assortments = assortments.substring(0, begin) + assortments.substring(end);

            //fieldValues.put("assortments", StringUtils.isNotEmpty(assortments) ? assortments : null);
            begin = assortments.indexOf(assortmentDid.toString());
        }
        System.out.println("New assortments: " + assortments);
    }

    private static String escapeSpecialRegexChars(String str) {
        //+ - = && || > < ! ( ) { } [ ] ^ " ~ * ? : \ /
        //Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[\\{\\}\\(\\)\\[\\]\\.\\+\\*\\?\\^\\$\\\\\\|]");
        Pattern SPECIAL_REGEX_CHARS = Pattern.compile("\\/");
        String escaped_str = SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\/");
        //String escaped_str = str.replaceAll("\\/", "\\\\/");
        System.out.println("Given: " + str);
        System.out.println("After escaped: " + escaped_str);
        
            
        return escaped_str;
    }
}
