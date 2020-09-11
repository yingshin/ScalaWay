/*
VM options: -Xss256k

stack length:2213
Exception in thread "main" java.lang.StackOverflowError
	at JavaVMStackSOF.stackLeak(JavaVMStackSOF.java:6)
 */
public class JavaVMStackSOF {
    private int stackLength = 1;

    public void stackLeak() {
        stackLength++;
        stackLeak();
    }

    public static void main(String[] args) throws Throwable {
        JavaVMStackSOF oom = new JavaVMStackSOF();
        try {
            oom.stackLeak();
        } catch (Throwable e) {
            System.out.println("stack length:" + oom.stackLength);
            throw e;
        }
    }
}
