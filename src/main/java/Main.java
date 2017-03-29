public class Main {

    public static void main(String[] args) {
        //File currentDirectory = new File(new File(".").getAbsolutePath());
        //String temp = currentDirectory.getAbsolutePath();
        //final String data_path = temp.substring(0, temp.length() - 2) + "/";
        //System.out.println(data_path);

        MakeFilesBySerialNumber istance=new MakeFilesBySerialNumber();
        istance.compute();
    }
}
