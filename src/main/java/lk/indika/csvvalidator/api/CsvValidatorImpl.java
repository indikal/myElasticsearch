package lk.indika.csvvalidator.api;

import com.opencsv.CSVReader;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CsvValidatorImpl implements CsvValidator {

    private String filename;
    private CSVReader csvReader;
    private int noOfFields;
    private List<Field> listFieldDefs;
    private Map<Integer, Field> map = new HashMap<Integer, Field>();
    private String delimiter;
    private StringBuilder errors = new StringBuilder();
    private int count = 1;
    private int errorsInitialSize;
    private int totalErrors = 0;
    private boolean jobDone = false;
    private boolean success = false;

    public CsvValidatorImpl(CSVReader csvReader, String filename, List<Field> listFieldDefs, String delimiter) {
        this.csvReader = csvReader;
        this.filename = filename;
        this.noOfFields = listFieldDefs.size();
        this.listFieldDefs = listFieldDefs;
        this.delimiter = delimiter;
        errorsInitialSize = this.errors.length();
    }

    public CsvValidatorImpl(String filename, List<Field> listFieldDefs, String delimiter) throws UnsupportedEncodingException {
        this.filename = filename;
        csvReader = new CSVReader(
            new InputStreamReader(getInputStreamFromCVS(filename),"UTF-8"), 
            delimiter.charAt(0));
        this.noOfFields = listFieldDefs.size();
        this.listFieldDefs = listFieldDefs;
        this.delimiter = delimiter;
        errorsInitialSize = this.errors.length();
    }
    
    private InputStream getInputStreamFromCVS(String filename) {
        InputStream is = null;
        try {
            File file = new File(filename);
            is = new FileInputStream(file);
            StringBuffer sb = new StringBuffer();
            String line = null;
            BufferedReader bReader = new BufferedReader(new InputStreamReader(is));
            while ((line = bReader.readLine()) != null) {
                sb.append(line.replaceAll("\"", "\"\"").replaceAll("\'", "\'\'"));
                sb.append("\r\n");
            }   if (sb.length() <= 0) {
                return null;
            } else {
                return new ByteArrayInputStream(sb.toString().getBytes());
            }
        } catch (Exception ex) {
            Logger.getLogger(CsvValidatorImpl.class.getName()).log(Level.SEVERE, null, ex);
        } finally {
            try {
                is.close();
            } catch (IOException ex) {
                Logger.getLogger(CsvValidatorImpl.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }

    @Override
    public boolean isValid() {
        if (!jobDone) {
            getValidationDetails();
        }
        return this.success;
    }

    @Override
    public String getValidationDetails() {
        if (jobDone) {
            return this.errors.toString();
        }
        try {
            listToMap(); //add the field defs to a map
            /*FileInputStream fstream = new FileInputStream(filename);
            DataInputStream in = new DataInputStream(fstream);
            BufferedReader br = new BufferedReader(new InputStreamReader(in));*/
            String[] strLine;
            while ((strLine = csvReader.readNext()) != null) {
                validateLine(strLine);
                count++;
            }
            //in.close();
        } catch (Exception e) {
            this.errors.append("Error: ");
            this.errors.append(e.getMessage());
            this.errors.append("\n");
        }

        jobDone = true;

        if (this.errors.length() == errorsInitialSize) {
            this.success = true;

        }

        this.errors.append("Filename : ");
        this.errors.append(filename);
        this.errors.append("\n");
        this.errors.append("Total Lines # ");
        this.errors.append(count-1);
        this.errors.append("\n");
        this.errors.append("Total Errors # ");
        this.errors.append(totalErrors);
        return this.errors.toString();

    }

    private void validateLine(String[] tokens) {
        if (tokens == null || tokens.length == 0) {
            this.errors.append("line ");
            this.errors.append(this.count);
            this.errors.append(" is empty ");
            this.errors.append("\n");
            totalErrors++;
        }
        //String[] tokens = line.split(delimiter, -1);
        
        if (tokens.length != noOfFields) {
            this.errors.append("Error: ");
            this.errors.append("Line: ");
            this.errors.append(this.count);
            this.errors.append(" is invalid, contains ");
            this.errors.append(tokens.length);
            this.errors.append(" required ");
            this.errors.append(this.noOfFields);
            this.errors.append(" fields ");
            this.errors.append("\n");
            totalErrors++;
        } else if (count == 1) {
            if (!isFirstLineHeader(tokens)) {
                Integer index = 1;
                for (String token : tokens) {
                    Field f = map.get(index);
                    handle(token, f);
                    index++;
                }
            }
        } else {
            // each field should be of given type
            Integer index = 1;
            for (String token : tokens) {
                Field f = map.get(index);
                handle(token, f);
                index++;
            }
            if ( tokens.length != noOfFields) {
                Field f = map.get(index);
                handle("", f);
            }
        } 
    }

    private boolean isFirstLineHeader(String[] tokens) {
        Integer index = 1;
        for (String t : tokens) {
            String tt = t.replaceAll("_", "");
            Field f = map.get(index);
            index++;
            if (tt.equalsIgnoreCase(f.getName().replaceAll(" ", ""))) {
                return true;
            }
        }
        return false;
    }

    private void handle(String token, Field f) {
        try {
            // handle optional
            if (f.isIsOptional() && (token == null || token.trim().length() == 0)) {
                return;
            }

            if (token == null || token.trim().length() == 0) {
                throw new RuntimeException();
            }

            if (f.getType().equals(Type.NUMBER)) {
                checkRegex(f, token);
                Double.parseDouble(token);
            } else if (f.getType().equals(Type.TEXT)) {
                checkRegex(f, token);
            } else if (f.getType().equals(Type.DATE)) {
                if (f.getRegex() != null && f.getRegex().trim().length() > 0) {
                    DateFormat df = new SimpleDateFormat(f.getRegex());
                    df.parse(token);
                }
            }

        } catch (Exception ex) {
            this.errors.append("Error: ");
            this.errors.append("Line: ");
            this.errors.append(this.count);
            this.errors.append(" required ");
            this.errors.append(f.getRegex() != null ?  "valid value" : f.getType());
            this.errors.append(", found '");
            this.errors.append(token);
            this.errors.append("'");
            this.errors.append(" for column: ");
            this.errors.append(f.getIndex());
            this.errors.append(f.getName() != null ? " (" + f.getName()  + ")" : "");
            //this.errors.append(" indexed at ");
            //this.errors.append(f.getIndex());
            this.errors.append(" \n");
            totalErrors++;
        }
    }

    private void checkRegex(Field f, String token) throws RuntimeException {
        if (f.getRegex() != null && f.getRegex().trim().length() > 0) {
            Pattern p = Pattern.compile(f.getRegex());
            Matcher m = p.matcher(token);
            if (!m.matches()) {
                throw new RuntimeException();
            }
        }
    }

    private void listToMap() {
        Collections.sort(listFieldDefs);
        Set<Field> set = new HashSet<Field>(listFieldDefs);

        if (listFieldDefs.get(0).getIndex() != 1 || listFieldDefs.get(listFieldDefs.size() - 1).getIndex() != listFieldDefs.size()
                || set.size() < listFieldDefs.size()) {
            this.errors.append("Invalid indexes found if you have 10 fields your index starts from 0 to 9\n");
            this.errors.append("You need to correct these indexes before we can proceed. \n");
            this.errors.append("You supplied Field indexes - ");
            this.errors.append(listFieldDefs);
            totalErrors++;
        }

        for (Field f : listFieldDefs) {
            map.put(f.getIndex(), f);
        }
    }
}
