package concurrent.demo;

public class DeadLock {

    public static void main(String[] args) {
        new Thread(new Runnable() {
            public void run() {
                try {
                    new DeadLock().resource1();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();


        new Thread(new Runnable() {
            public void run() {
                try {
                    new DeadLock().resource2();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    void resource1() throws InterruptedException {
        synchronized ("resource1") {
            System.out.println("获取资源1");
            Thread.sleep(1000);
            resource2();
        }
    }

    void resource2() throws InterruptedException {
        synchronized ("resource2") {
            System.out.println("获取资源2");
            Thread.sleep(1000);
            resource1();
        }
    }
}
