/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package lk.indika;

/**
 *
 * @author indika
 */
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.util.Locale;

public class TestParseDouble {
	public static void main(String[] args) {
		String[] nums = new String[] { "12", "12.5", "12,5", "12 5", "12,5.5", "12dd.5", "12df,5", "12.5ee", "df12.5" };

        Locale locale = Locale.getDefault();
		parse(nums, locale);
        
        locale = new Locale("sv");
        parse(nums, locale);
	}
    
    private static void parse(String[] nums, Locale locale) {
        System.out.println("Testing for " + locale.getDisplayName() + " locale");
		
        for (String num : nums) {
			try {
				double dbl_num = parseDouble(num, locale);
				System.out.println(num + ": parsed as valid number");
			} catch (NumberFormatException e) {
				System.out.println(num + ": not a number");
			}
		}
    }
	
	public static double parseDouble(String value, Locale locale) throws NumberFormatException {
        //check for values of each locale.
        DecimalFormatSymbols dfs = new DecimalFormatSymbols(locale);
        if(locale.getLanguage().equals("sv")) {
            //relplace the whitespace with locale specific group seperator.
            char whiteSpace = ' ';
            value = value.replace(whiteSpace, dfs.getGroupingSeparator());
        }

        NumberFormat nf = NumberFormat.getInstance(locale);
        //nf.setGroupingUsed(false);
        ParsePosition pos = new ParsePosition(0);

        Number parsed = nf.parse(value, pos);
        // pos is set AFTER the last parsed position
        
        if (pos.getIndex() == value.length()) {
        	return parsed.doubleValue();
        } else {
        	throw new NumberFormatException();
        }
    }
}
