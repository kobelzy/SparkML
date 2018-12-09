public class GCTest {
    public static void main(String[] args) {
        byte[] allocation1;
        //年轻代共38400K
        //eden区有33280，这里占据了99%=33194
        allocation1 = new byte[23900*1024];
        byte[] allocation2;
        allocation2 = new byte[900*1024];
    }
}
