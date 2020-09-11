import sun.misc.Unsafe;

import java.lang.reflect.Field;

/*
-Xmx20M -XX:MaxDirectMemorySize=10M
 */
public class DirectMemoryOOM {
    private static final int _1MB = 1024 * 1024;

    public static void main(String[] args) throws Exception {
        Field unsafeField = Unsafe.class.getDeclaredFields()[0];
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);

        while (true) {
            long s = unsafe.allocateMemory(_1MB);
            unsafe.putChar(s, 'a');
            unsafe.putChar(s + _1MB - 2, 'a');
            System.out.println("memory address=" + s);
            Thread.sleep(1000);
        }
    }
}
