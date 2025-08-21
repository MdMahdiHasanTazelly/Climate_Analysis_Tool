import java.io.*;
import java.nio.file.*;
import java.util.*;

public class climate_analysis {
    /* ---------- Data Model ---------- */
    static class Record {
        String country; int year;
        double tempAnom, co2, gdp, extreme, urban, deforest;
        Record(String c,int y,double t,double co,double g,double e,double u,double d){
            country=c; year=y; tempAnom=t; co2=co; gdp=g; extreme=e; urban=u; deforest=d;
        }
    }

    /* ---------- Storage ---------- */
    private final ArrayList<Record> rows = new ArrayList<>();
    private int[] yearIdx = new int[0]; // indices into rows sorted by year

    /* ---------- CSV Loading ---------- */
    public void loadCsv(String path) throws IOException {
        try(BufferedReader br = Files.newBufferedReader(Paths.get(path))) {
            String header = br.readLine();
            //String header = "./data.csv";
            if (header == null) throw new IOException("Empty CSV");
            String[] H = splitCsv(header);
            HashMap<String,Integer> hi = new HashMap<>();
            for (int i=0;i<H.length;i++) hi.put(norm(H[i]), i);

            int iC  = headerIndex(hi, "country");
            int iY  = headerIndex(hi, "year");
            int iT  = headerIndex(hi, "temperature_anomaly","temperature anomaly","temp_anomaly");
            int iCO = headerIndex(hi, "co2_emissions","co2 emissions","co2");
            int iG  = headerIndex(hi, "gdp");
            int iE  = headerIndex(hi, "extreme_weather_events","extreme weather events","extremeevents");
            int iU  = headerIndex(hi, "urbanization","urbanisation");
            int iD  = headerIndex(hi, "deforestation");
            if (iC<0 || iY<0) throw new IOException("CSV must contain Country and Year");

            for (String line; (line = br.readLine()) != null; ) {
                if (line.trim().isEmpty()) continue;
                String[] v = splitCsv(line);
                String c = get(v, iC);
                int y = parseInt(get(v, iY));
                if (c.isEmpty() || y==0) continue;
                rows.add(new Record(
                    c, y,
                    parseDouble(get(v, iT)),
                    parseDouble(get(v, iCO)),
                    parseDouble(get(v, iG)),
                    parseDouble(get(v, iE)),
                    parseDouble(get(v, iU)),
                    parseDouble(get(v, iD))
                ));
            }
        }
        yearIdx = new int[rows.size()];
        for (int i=0;i<yearIdx.length;i++) yearIdx[i]=i;
        mergeSortYearIndex(yearIdx,0,yearIdx.length-1);
    }

    /* ---------- CSV Helpers ---------- */
    private static String norm(String s){
        String t = (s==null?"":s).trim().toLowerCase(Locale.ROOT);
        StringBuilder b = new StringBuilder();
        for (int i=0;i<t.length();i++){ char ch=t.charAt(i); if(ch!=' '&&ch!='_'&&ch!='-') b.append(ch); }
        return b.toString();
    }
    private static int headerIndex(Map<String,Integer> m, String... aliases){
        for (String a: aliases){ Integer i = m.get(norm(a)); if (i!=null) return i; } return -1;
    }
    private static String[] splitCsv(String line){
        ArrayList<String> out=new ArrayList<>(); StringBuilder cur=new StringBuilder(); boolean q=false;
        for (int i=0;i<line.length();i++){ char ch=line.charAt(i);
            if (ch=='\"'){ if(q && i+1<line.length() && line.charAt(i+1)=='\"'){cur.append('\"'); i++;} else q=!q; }
            else if (ch==',' && !q){ out.add(cur.toString()); cur.setLength(0); }
            else cur.append(ch);
        } out.add(cur.toString()); return out.toArray(new String[0]);
    }
    private static String get(String[] v,int idx){ return (idx>=0 && idx<v.length) ? v[idx].trim() : ""; }
    private static int parseInt(String s){ try{ return (s==null||s.isEmpty())?0:Integer.parseInt(s); }catch(Exception e){ return 0; } }
    private static double parseDouble(String s){ try{ if(s==null) return Double.NaN; String t=s.trim(); return t.isEmpty()||t.equalsIgnoreCase("nan")?Double.NaN:Double.parseDouble(t); }catch(Exception e){ return Double.NaN; } }
    private static double nzNegInf(double x){ return Double.isNaN(x)?Double.NEGATIVE_INFINITY:x; }

    /* ---------- Year Index (stable merge sort + bounds) ---------- */
    private void mergeSortYearIndex(int[] a,int l,int r){
        if (l>=r) return; int m=(l+r)>>>1; mergeSortYearIndex(a,l,m); mergeSortYearIndex(a,m+1,r);
        int n1=m-l+1,n2=r-m; int[] L=new int[n1], R=new int[n2];
        System.arraycopy(a,l,L,0,n1); System.arraycopy(a,m+1,R,0,n2);
        int i=0,j=0,k=l;
        while(i<n1&&j<n2){ if(rows.get(L[i]).year<=rows.get(R[j]).year) a[k++]=L[i++]; else a[k++]=R[j++]; }
        while(i<n1) a[k++]=L[i++]; while(j<n2) a[k++]=R[j++];
    }
    private int lowerBound(int y){ int lo=0,hi=yearIdx.length; while(lo<hi){ int m=(lo+hi)>>>1; if(rows.get(yearIdx[m]).year<y) lo=m+1; else hi=m; } return lo; }
    private int upperBound(int y){ int lo=0,hi=yearIdx.length; while(lo<hi){ int m=(lo+hi)>>>1; if(rows.get(yearIdx[m]).year<=y) lo=m+1; else hi=m; } return lo; }

    /* ---------- Searching ---------- */
    public ArrayList<Record> searchByCountryLinear(String name){
        String q = (name==null?"":name).trim().toLowerCase(Locale.ROOT);
        ArrayList<Record> out=new ArrayList<>();
        for (Record r: rows) if (r.country!=null && r.country.toLowerCase(Locale.ROOT).equals(q)) out.add(r);
        return out;
    }
    public ArrayList<Record> searchByYearRangeBinary(int start,int end){
        if (end<start){ int t=start; start=end; end=t; }
        int L=lowerBound(start), U=upperBound(end);
        ArrayList<Record> out=new ArrayList<>(U-L);
        for (int i=L;i<U;i++) out.add(rows.get(yearIdx[i]));
        return out;
    }

    /* ---------- Sorting ---------- */
    private ArrayList<Record> mergeSortBy(ArrayList<Record> a, boolean asc, int key){ // key 0=temp, 1=gdp
        if (a.size()<=1) return a;
        int m=a.size()/2;
        ArrayList<Record> L = mergeSortBy(new ArrayList<>(a.subList(0,m)),asc,key);
        ArrayList<Record> R = mergeSortBy(new ArrayList<>(a.subList(m,a.size())),asc,key);
        ArrayList<Record> o = new ArrayList<>(a.size());
        int i=0,j=0;
        while(i<L.size()&&j<R.size()){
            double x = key==0? nzNegInf(L.get(i).tempAnom) : nzNegInf(L.get(i).gdp);
            double y = key==0? nzNegInf(R.get(j).tempAnom) : nzNegInf(R.get(j).gdp);
            boolean take = asc ? x<=y : x>=y;
            o.add(take?L.get(i++):R.get(j++));
        }
        while(i<L.size()) o.add(L.get(i++));
        while(j<R.size()) o.add(R.get(j++));
        return o;
    }
    private void quickSortCo2(ArrayList<Record> a,int l,int r){ // DESC
        if (l>=r) return; double p = nzNegInf(a.get(r).co2); int i=l;
        for (int j=l;j<r;j++) if (nzNegInf(a.get(j).co2)>p){ Collections.swap(a,i,j); i++; }
        Collections.swap(a,i,r); quickSortCo2(a,l,i-1); quickSortCo2(a,i+1,r);
    }

    /* ---------- Tasks from rubric ---------- */
    public ArrayList<Map.Entry<String,Double>> topNCo2EmittersQuickSort(int year,int N){
        ArrayList<Record> subset=new ArrayList<>(); for (Record r:rows) if (r.year==year) subset.add(r);
        if (subset.isEmpty()) return new ArrayList<>();
        quickSortCo2(subset,0,subset.size()-1);
        int n=Math.min(N,subset.size()); ArrayList<Map.Entry<String,Double>> out=new ArrayList<>(n);
        for (int i=0;i<n;i++) out.add(new AbstractMap.SimpleEntry<>(subset.get(i).country, subset.get(i).co2));
        return out;
    }
    public ArrayList<Map.Entry<String,Double>> extremeEventsRanking(int k, boolean highest){
        HashMap<String,Double> sum=new HashMap<>();
        for (Record r:rows) sum.put(r.country, sum.getOrDefault(r.country,0.0) + (Double.isNaN(r.extreme)?0:r.extreme));
        ArrayList<Map.Entry<String,Double>> L=new ArrayList<>(sum.entrySet());
        L.sort(highest? (a,b)->Double.compare(b.getValue(),a.getValue())
                      : (a,b)->Double.compare(a.getValue(),b.getValue()));
        return (k<=0||k>=L.size())?L:new ArrayList<>(L.subList(0,k));
    }
    public HashMap<String,Double> averageMetrics(String country){
        ArrayList<Record> ls = searchByCountryLinear(country);
        if (ls.isEmpty()) return null;
        double co=0, t=0, g=0; int n=ls.size();
        for (Record r:ls){ if(!Double.isNaN(r.co2)) co+=r.co2; if(!Double.isNaN(r.tempAnom)) t+=r.tempAnom; if(!Double.isNaN(r.gdp)) g+=r.gdp; }
        HashMap<String,Double> out=new HashMap<>(); out.put("CO2_Emissions",co/n); out.put("Temperature_Anomaly",t/n); out.put("GDP",g/n); return out;
    }
    public void showUrbanizationDeforestation(String country){
        ArrayList<Record> ls = searchByCountryLinear(country);
        if (ls.isEmpty()){ System.out.println("No records for "+country); return; }
        boolean hasU=false, hasD=false; double su=0, sd=0; int cu=0, cd=0;
        for (Record r:ls){ if(!Double.isNaN(r.urban)){ hasU=true; su+=r.urban; cu++; } if(!Double.isNaN(r.deforest)){ hasD=true; sd+=r.deforest; cd++; } }
        System.out.println("\nUrbanization & Deforestation for "+country+":");
        System.out.println(hasU? String.format(Locale.ROOT,"Average Urbanization: %.4f", su/Math.max(1,cu)) : "Urbanization column not found.");
        System.out.println(hasD? String.format(Locale.ROOT,"Average Deforestation: %.4f", sd/Math.max(1,cd)) : "Deforestation column not found.");
    }

    /* ---------- Printing ---------- */
    private static void printRecords(List<Record> list,String title,int limit){
        System.out.println("\n=== "+title+" ===");
        System.out.println("Country | Year | TempAnom | CO2 | GDP | Extreme");
        int n = (limit<=0)?list.size():Math.min(limit,list.size());
        for (int i=0;i<n;i++){ Record r=list.get(i);
            System.out.printf(Locale.ROOT,"%s | %d | %.4f | %.4f | %.2f | %.2f%n", r.country,r.year,r.tempAnom,r.co2,r.gdp,r.extreme);
        }
        if (n<list.size()) System.out.println("... ("+(list.size()-n)+" more)");
    }
    private static void printPairs(List<Map.Entry<String,Double>> pairs,String title){
        System.out.println("\n=== "+title+" ===");
        if (pairs.isEmpty()){ System.out.println("(no results)"); return; }
        for (int i=0;i<pairs.size();i++){ Map.Entry<String,Double> e=pairs.get(i);
            System.out.printf(Locale.ROOT,"%d. %s -> %.4f%n", i+1, e.getKey(), e.getValue());
        }
    }

    /* ---------- Main: menu + runtimes ---------- */
    public static void main(String[] args){
        Scanner sc = new Scanner(System.in);
        //java_script app = new java_script();
        climate_analysis app = new climate_analysis();

        // System.out.print("Enter CSV file path (e.g., global_warming_dataset.csv): ");
        // String path = sc.nextLine().trim();
        String path = "data.csv";
        try {
            long t0=System.nanoTime(); app.loadCsv(path); long t1=System.nanoTime();
            System.out.printf(Locale.ROOT,"Loaded %d rows in %.3f ms%n", app.rows.size(), (t1-t0)/1e6);
        } catch(IOException e){ System.out.println("Failed to read CSV: "+e.getMessage()); return; }

        if (app.rows.isEmpty()){ System.out.println("No valid rows loaded. Check CSV headers/data."); return; }

        while(true){
            System.out.println("\n1) Search by Country (Linear)");
            System.out.println("2) Search by Year Range (Binary)");
            System.out.println("3) Highest/Lowest K by Extreme Events");
            System.out.println("4) Top-N CO2 in a Year (QuickSort DESC)");
            System.out.println("5) Sort by Temperature Anomaly (MergeSort)");
            System.out.println("6) Sort by GDP in a Year (MergeSort)");
            System.out.println("7) Average Metrics for Country");
            System.out.println("8) Country Urbanization/Deforestation");
            System.out.println("9) Exit");
            System.out.print("Choice: ");
            String cs = sc.nextLine().trim(); int ch=0; try{ ch=Integer.parseInt(cs);}catch(Exception ignored){}

            if (ch==1){
                System.out.print("Country (exact label, e.g., Country_103): ");
                String c = sc.nextLine().trim(); long a=System.nanoTime();
                ArrayList<Record> res = app.searchByCountryLinear(c); long b=System.nanoTime();
                if (res.isEmpty()) System.out.println("No records for "+c); else printRecords(res,"Records for "+c,50);
                System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==2){
                System.out.print("Start year: "); int s=Integer.parseInt(sc.nextLine().trim());
                System.out.print("End year: "); int e=Integer.parseInt(sc.nextLine().trim());
                long a=System.nanoTime(); ArrayList<Record> res=app.searchByYearRangeBinary(s,e); long b=System.nanoTime();
                if (res.isEmpty()) System.out.println("No records in range "+s+" - "+e); else printRecords(res,"Year Range "+s+" - "+e,50);
                System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==3){
                System.out.print("K: "); int k=Integer.parseInt(sc.nextLine().trim());
                System.out.print("Highest(true) or Lowest(false): "); boolean hi=Boolean.parseBoolean(sc.nextLine().trim());
                long a=System.nanoTime(); ArrayList<Map.Entry<String,Double>> res=app.extremeEventsRanking(k,hi); long b=System.nanoTime();
                printPairs(res, (hi?"Top ":"Bottom ")+k+" by Extreme Events"); System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==4){
                System.out.print("Year: "); int y=Integer.parseInt(sc.nextLine().trim());
                System.out.print("Top N: "); int n=Integer.parseInt(sc.nextLine().trim());
                long a=System.nanoTime(); ArrayList<Map.Entry<String,Double>> res=app.topNCo2EmittersQuickSort(y,n); long b=System.nanoTime();
                if (res.isEmpty()) System.out.println("No records for year "+y); else printPairs(res,"Top "+n+" CO2 in "+y);
                System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==5){
                System.out.print("Ascending (true/false): "); boolean asc=Boolean.parseBoolean(sc.nextLine().trim());
                ArrayList<Record> copy=new ArrayList<>(app.rows); long a=System.nanoTime();
                ArrayList<Record> sorted=app.mergeSortBy(copy,asc,0); long b=System.nanoTime();
                printRecords(sorted,"Temperature Anomaly ("+(asc?"ASC":"DESC")+")",50); System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==6){
                System.out.print("Year: "); int y=Integer.parseInt(sc.nextLine().trim());
                System.out.print("Ascending (true/false): "); boolean asc=Boolean.parseBoolean(sc.nextLine().trim());
                ArrayList<Record> subset=new ArrayList<>(); for (Record r:app.rows) if (r.year==y) subset.add(r);
                if (subset.isEmpty()) { System.out.println("No records for year "+y); continue; }
                long a=System.nanoTime(); ArrayList<Record> sorted=app.mergeSortBy(subset,asc,1); long b=System.nanoTime();
                printRecords(sorted,"GDP "+y+" ("+(asc?"ASC":"DESC")+")",50); System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==7){
                System.out.print("Country: "); String c=sc.nextLine().trim();
                long a=System.nanoTime(); HashMap<String,Double> avg=app.averageMetrics(c); long b=System.nanoTime();
                if (avg==null) System.out.println("No records for "+c);
                else { System.out.println("\n=== Averages for "+c+" ===");
                       System.out.printf(Locale.ROOT,"CO2 Emissions: %.4f%n", avg.get("CO2_Emissions"));
                       System.out.printf(Locale.ROOT,"Temperature Anomaly: %.4f%n", avg.get("Temperature_Anomaly"));
                       System.out.printf(Locale.ROOT,"GDP: %.2f%n", avg.get("GDP")); }
                System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==8){
                System.out.print("Country: "); String c=sc.nextLine().trim();
                long a=System.nanoTime(); app.showUrbanizationDeforestation(c); long b=System.nanoTime();
                System.out.printf(Locale.ROOT,"Time: %.3f ms%n",(b-a)/1e6);

            } else if (ch==9){
                System.out.println("Exiting..."); return;
            } else {
                System.out.println("Invalid choice.");
            }
        }
    }
}
