package nl.b3p.ags2psql;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.*;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.methods.RequestBuilder;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author matthijsln
 */
public class Main {

    private static boolean verifySsl = true;

    private static String agsUrl;
    private static String agsToken;
    private static JSONObject agsInfo;

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption("h", false, "Show this help");
        options.addOption(Option.builder("db")
                .required()
                .hasArg()
                .argName("jdbc-url")
                .desc("JDBC URL for output database. Set PGUSER and PGPASSWORD environment variables for credentials. Include ?currentSchema=myschema to set output schema")
                .build());
        options.addOption(Option.builder("url")
                .required()
                .hasArg()
                .desc("ArcGIS server URL (ending with /MapServer or /FeatureServer), set AGSUSER and AGSPASSWORD environment variables for credentials.")
                .build());
        options.addOption(Option.builder("tokenURL")
                .hasArg()
                .desc("ArcGIS token URL (get from server info /arcgis/rest/info)")
                .build());
        options.addOption(Option.builder("nosslverify")
                .desc("Do not verify SSL certificate")
                .build());

        OptionGroup commands = new OptionGroup();
        commands.setRequired(true);
        commands.addOption(Option.builder("table")
                    .hasArgs()
                    .desc("Convert table(s)")
                    .build());
        options.addOptionGroup(commands);

        // No layer converter yet; use this code for geometries:
        // https://github.com/flamingo-geocms/flamingo/tree/master/viewer-commons/src/main/java/nl/b3p/geotools/data/arcgis

        return options;
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("ags2psql", options );
    }

    public static void main(String[] args) throws Exception {

        Options options = buildOptions();
        CommandLine cl = null;
        try {
            CommandLineParser parser = new DefaultParser();

            cl = parser.parse(options, args);
        } catch(ParseException e) {
            System.out.printf("%s\n\n", e.getMessage());

            printHelp(options);
            System.exit(1);
        }

        if(cl.hasOption("nosslverify")) {
            verifySsl = false;
        }

        Class.forName("org.postgresql.Driver");

        String user = System.getenv("PGUSER");
        String pass = System.getenv("PGPASSWORD");
        Connection c = DriverManager.getConnection(cl.getOptionValue("db"), user, pass);

        agsUrl = cl.getOptionValue("url");

        String agsUser = System.getenv("AGSUSER");
        String agsPass = System.getenv("AGSPASSWORD");

        if(agsUser != null && agsPass != null) {
            if(!cl.hasOption("tokenURL")) {
                System.err.println("tokenURL option required to use ArcGIS server authentication");
                System.exit(1);
            }
            agsToken = getAgsToken(cl.getOptionValue("tokenURL"), agsUser, agsPass);
        }

        agsInfo = agsJsonRequest(agsUrl, null);

        try {
            if(cl.hasOption("table")) {
                cmdConvertTables(c, cl.getOptionValue("url"), cl.getOptionValues("table"));
            }
            System.err.println("No command specified");
            System.exit(1);
        } catch(Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static CloseableHttpClient createHttpClient() throws Exception {
        HttpClientBuilder builder = HttpClients.custom();
        if(!verifySsl) {
            builder.setSslcontext(new SSLContextBuilder().loadTrustMaterial(null, new TrustStrategy() {
                @Override
                public boolean isTrusted(X509Certificate[] xcs, String string) throws CertificateException {
                    return true;
                }

            }).build());
        }
        return builder.build();
    }

    private static String getAgsToken(String tokenURL, String user, String pass) throws Exception {
        RequestBuilder builder = RequestBuilder.post()
                .setUri(tokenURL)
                .addParameter("f", "json")
                .addParameter("username", user)
                .addParameter("password", pass);
        final HttpUriRequest req = builder.build();

        try(CloseableHttpClient client = createHttpClient()) {
            final String content = client.execute(req, new ResponseHandler<String>() {
                @Override
                public String handleResponse(HttpResponse hr) throws ClientProtocolException, IOException {
                    try {
                        return IOUtils.toString(hr.getEntity().getContent(), "UTF-8");
                    } catch(IOException e) {
                        return "Exception " + e.getClass() + ": " + e.getMessage();
                    }
                }
            });

            //System.out.println("Got token response: " + content);
            JSONObject j = new JSONObject(content);
            return j.getString("token");
        }
    }

    private static JSONObject agsJsonRequest(String url, Map<String,String> params) throws Exception {
        RequestBuilder builder = RequestBuilder.post()
                .setUri(url)
                .addParameter("f", "json")
                .addParameter("token", agsToken);
        if(params != null) {
            for(Map.Entry<String,String> param: params.entrySet()) {
                builder.addParameter(param.getKey(), param.getValue());
            };
        }
        final HttpUriRequest req = builder.build();

        try(CloseableHttpClient client = createHttpClient()) {
            final MutableBoolean hasException = new MutableBoolean(false);
            final String content = client.execute(req, new ResponseHandler<String>() {
                @Override
                public String handleResponse(HttpResponse hr) throws ClientProtocolException, IOException {
                    try {
                        return IOUtils.toString(hr.getEntity().getContent(), "UTF-8");
                    } catch(IOException e) {
                        hasException.setTrue();
                        return "Exception " + e.getClass() + ": " + e.getMessage();
                    }
                }
            });

            //System.out.println("Got response: " + content);

            if(hasException.booleanValue()) {
                throw new Exception("Error on request " + url + ": " + content);
            }
            JSONObject o;
            try {
                o = new JSONObject(content);
            } catch(JSONException e) {
                throw new Exception("Error parsing JSON from request " + url + ": " + e.getClass() + ": " + e.getMessage());
            }

            if(o.has("error")) {
                throw new Exception("Error on request " + url + ": " + content);
            }

            return o;
        }
    }

    private static void cmdConvertTables(Connection c, String url, String[] tables) throws Exception {
        System.err.println("Converting tables: " + Arrays.toString(tables));

        JSONArray serviceTables = agsInfo.optJSONArray("tables");

        for(String tableName: tables) {
            Integer tableId = null;

            if(serviceTables != null) {
                for(int i = 0; i < serviceTables.length(); i++) {
                    JSONObject table = serviceTables.getJSONObject(i);
                    if(tableName.equals(table.optString("name"))) {
                        tableId = table.getInt("id");
                    }
                }
            }
            if(tableId == null) {
                System.err.println("Table not found: " + tableName);
                System.exit(1);
            }
            System.out.printf("Converting table ID %d, '%s'... ", tableId, tableName);
            boolean first = true;
            int resultOffset = 0;
            while(true) {
                Map<String,String> params = new HashMap();
                params.put("where", "1=1");
                params.put("outFields", "*");
                params.put("resultOffset", resultOffset + "");
                JSONObject results = agsJsonRequest(agsUrl + "/" + tableId + "/query", params);
                //System.out.printf("JSON: %s\n", results.toString());
                convertAgsJsonToTable(c, tableName, first, results);
                first = false;

                if(results.optBoolean("exceededTransferLimit", false)) {
                    // Doc says even if features length is zero to request next page
                    // (with same resultOffset???)
                    resultOffset += results.getJSONArray("features").length();
                } else {
                    break;
                }
            }
        }

        System.exit(0);
    }

    private static void convertAgsJsonToTable(Connection c, String name, boolean recreateTable, JSONObject json) throws SQLException {
        JSONArray features = json.getJSONArray("features");
        if(features.length() == 0) {
            System.out.printf("no features, skipping");
            return;
        }
        int i = name.lastIndexOf('.');
        if(i != -1) {
            name = name.substring(i+1);
        }
        name = name.toLowerCase();
        System.out.printf("got %d features, %s table '%s', ", features.length(), recreateTable ? "creating/replacing" : "appending to", name);

        String sql = "create table " + name + "(";
        String insertSql = "insert into " + name + "(";
        JSONArray fields = json.getJSONArray("fields");
        for(int j = 0; j < fields.length(); j++) {
            JSONObject field = fields.getJSONObject(j);
            String fieldName = field.getString("name");
            String fieldType = field.getString("type");
            if(j > 0) {
                sql += ",\n    ";
                insertSql += ", ";
            }
            String sqlType = "varchar";
            switch(fieldType) {
                case "esriFieldTypeOID": sqlType = "integer primary key"; break;
            }
            sql += fieldName + " " + sqlType;
            insertSql += fieldName;
        }
        sql += ");";
        insertSql += ") values(" + new String(new char[fields.length()]).replace("\0", ", ?").substring(2) + ");";

        QueryRunner qr = new QueryRunner();
        if(recreateTable) {
            qr.update(c, "drop table if exists " + name);
            qr.update(c, sql);
        }

        for(int fid = 0; fid < features.length(); fid++) {
            JSONObject feature = features.getJSONObject(fid);
            JSONObject attributes = feature.getJSONObject("attributes");
            Object[] values = new Object[fields.length()];
            for(int j = 0; j < fields.length(); j++) {
                Object value = attributes.get(fields.getJSONObject(j).getString("name"));
                if(value == null || value == JSONObject.NULL) {
                    values[j] = (String)null;
                } else {
                    values[j] = value;
                }
            }
            qr.update(c, insertSql, values);
        }
        System.out.println("done.");
    }
}
