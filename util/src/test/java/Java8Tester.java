import java.util.Arrays;
import java.util.Comparator;

/**
 * Lambda 表达式，也可称为闭包，它是推动 Java 8 发布的最重要新特性。
 *
 * Lambda 允许把函数作为一个方法的参数（函数作为参数传递进方法中）。
 *
 * 使用 Lambda 表达式可以使代码变的更加简洁紧凑。
 *
 *
 * <p>
 *     语法
 * lambda 表达式的语法格式如下：
 *
 * (parameters) -> expression
 * 或
 * (parameters) ->{ statements; }
 * </p>
 *
 * <p>
 *     以下是lambda表达式的重要特征:
 *
 * 可选类型声明：不需要声明参数类型，编译器可以统一识别参数值。
 * 可选的参数圆括号：一个参数无需定义圆括号，但多个参数需要定义圆括号。
 * 可选的大括号：如果主体包含了一个语句，就不需要使用大括号。
 * 可选的返回关键字：如果主体只有一个表达式返回值则编译器会自动返回值，大括号需要指定明表达式返回了一个数值。
 * </p>
 *
 * <p>
 *     Lambda 表达式实例
 * Lambda 表达式的简单例子:
 *
 * // 1. 不需要参数,返回值为 5
 * () -> 5
 *
 * // 2. 接收一个参数(数字类型),返回其2倍的值
 * x -> 2 * x
 *
 * // 3. 接受2个参数(数字),并返回他们的差值
 * (x, y) -> x – y
 *
 * // 4. 接收2个int型整数,返回他们的和
 * (int x, int y) -> x + y
 *
 * // 5. 接受一个 string 对象,并在控制台打印,不返回任何值(看起来像是返回void)
 * (String s) -> System.out.print(s)
 *
 * </p>
 *
 */
public class Java8Tester {
    public static void main(String args[]){
        Java8Tester tester = new Java8Tester();

        // 类型声明
        MathOperation addition = (int a, int b) -> a + b;

        // 不用类型声明
        MathOperation subtraction = (a, b) -> a - b;

        // 大括号中的返回语句
        MathOperation multiplication = (int a, int b) -> { return a * b; };

        // 没有大括号及返回语句
        MathOperation division = (int a, int b) -> a / b;

        System.out.println("10 + 5 = " + tester.operate(10, 5, addition));
        System.out.println("10 - 5 = " + tester.operate(10, 5, subtraction));
        System.out.println("10 x 5 = " + tester.operate(10, 5, multiplication));
        System.out.println("10 / 5 = " + tester.operate(10, 5, division));

        // 不用括号
        GreetingService greetService1 = message ->
                System.out.println("Hello " + message);

        // 用括号
        GreetingService greetService2 = (message) ->
                System.out.println("Hello " + message);

        greetService1.sayMessage("World");
        greetService2.sayMessage("Google");


        new Thread(() -> System.out.println("Hello world - thread !")).start();

        Runnable race2 = () -> System.out.println("Hello world - runable !");
        // 直接调用 run 方法(没开新线程哦!)
        race2.run();

        String[] players = {"Rafael Nadal", "Novak Djokovic",
                "Stanislas Wawrinka", "David Ferrer",
                "Roger Federer", "Andy Murray",
                "Tomas Berdych", "Juan Martin Del Potro",
                "Richard Gasquet", "John Isner"};
        Comparator<String> sortByName = (String s1, String s2) -> (s1.compareTo(s2));
        Arrays.sort(players, sortByName);
        Arrays.sort(players, (String s1, String s2) -> (s1.compareTo(s2)));

        // 1.2 使用 lambda expression 排序,根据 surname
        Comparator<String> sortBySurname = (String s1, String s2) ->
                ( s1.substring(s1.indexOf(" ")).compareTo( s2.substring(s2.indexOf(" ")) ) );
        Arrays.sort(players, sortBySurname);

        // 2.2 使用 lambda expression 排序,根据 name lenght
        Comparator<String> sortByNameLenght = (String s1, String s2) -> (s1.length() - s2.length());
        Arrays.sort(players, sortByNameLenght);
        Arrays.sort(players, (String s1, String s2) -> (s1.length() - s2.length()));

        // 3.2 使用 lambda expression 排序,根据最后一个字母
        Comparator<String> sortByLastLetter =
                (String s1, String s2) ->
                        (s1.charAt(s1.length() - 1) - s2.charAt(s2.length() - 1));
        Arrays.sort(players, sortByLastLetter);

        // 3.3 or this
        Arrays.sort(players, (String s1, String s2) -> (s1.charAt(s1.length() - 1) - s2.charAt(s2.length() - 1)));


    }

    interface MathOperation {
        int operation(int a, int b);
    }

    interface GreetingService {
        void sayMessage(String message);
    }

    private int operate(int a, int b, MathOperation mathOperation){
        return mathOperation.operation(a, b);
    }
}