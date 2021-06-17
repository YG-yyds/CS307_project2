package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Course;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.StudentService;

import java.sql.*;
import java.time.DayOfWeek;
import java.util.HashSet;
import java.util.Set;

public class Function {
    //user
    public static String getFullName(String firstName,String lastName){
        char[] first=firstName.toCharArray();
        char[] second=lastName.toCharArray();
        int first_flag=0;
        int second_flag=0;
        for (int i=0;i<first.length;i++){
            if ((first[i]>='A' && first[i]<='Z')|| (first[i]>='a' && first[i]<='z') || first[i]==' '){
                first_flag++;
            }
        }
        for (int i=0;i<second.length;i++){
            if ((second[i]>='A' && second[i]<='Z')|| (second[i]>='a' && second[i]<='z') || second[i]==' '){
                second_flag++;
            }
        }
        if (first_flag==first.length && second_flag==second.length){//英文名
            return firstName+" "+lastName;
        }else return firstName+lastName;
    }

    //course
    public static Course.CourseGrading getGradingByString(String grading){
        if (grading.equals("pf")){
            return Course.CourseGrading.PASS_OR_FAIL;
        }else return Course.CourseGrading.HUNDRED_MARK_SCORE;
    }

    public static String getGradingByCourseGrading(Course.CourseGrading grading){
        if (grading.equals(Course.CourseGrading.PASS_OR_FAIL)){
            return "pf";
        }else return "hm";
    }

    //course section class
    //dayOfWeek
    public static String dayOfWeekToString(DayOfWeek dayOfWeek){
        if (dayOfWeek==DayOfWeek.MONDAY){
            return "Monday";
        }else if (dayOfWeek==DayOfWeek.TUESDAY){
            return "Tuesday";
        }else if (dayOfWeek==DayOfWeek.WEDNESDAY){
            return "Wednesday";
        }else if (dayOfWeek==DayOfWeek.THURSDAY){
            return "Thursday";
        }else if (dayOfWeek==DayOfWeek.FRIDAY){
            return "Friday";
        }else if (dayOfWeek==DayOfWeek.SATURDAY){
            return "Saturday";
        }else return "Sunday";
    }

    public static DayOfWeek getDayOfWeek(String dayOfWeek){
        switch (dayOfWeek) {
            case "Monday":
                return DayOfWeek.MONDAY;
            case "Tuesday":
                return DayOfWeek.TUESDAY;
            case "Wednesday":
                return DayOfWeek.WEDNESDAY;
            case "Thursday":
                return DayOfWeek.THURSDAY;
            case "Friday":
                return DayOfWeek.FRIDAY;
            case "Saturday":
                return DayOfWeek.SATURDAY;
            default:
                return DayOfWeek.SUNDAY;
        }
    }

    public static DayOfWeek getDayOfWeekOfInt(int i){
        switch (i) {
            case 1:
                return DayOfWeek.MONDAY;
            case 2:
                return DayOfWeek.TUESDAY;
            case 3:
                return DayOfWeek.WEDNESDAY;
            case 4:
                return DayOfWeek.THURSDAY;
            case 5:
                return DayOfWeek.FRIDAY;
            case 6:
                return DayOfWeek.SATURDAY;
            default:
                return DayOfWeek.SUNDAY;
        }
    }

    //weekList
    public static Set<Short> getWeekList(String weekList){
        String[] split = weekList.split(",");
        Set<Short> set=new HashSet<>();
        for (int i=0;i<split.length;i++){
            set.add(Short.parseShort(split[i]));
        }
        return set;
    }

    public static boolean hasWeek(Set<Short> weekList,short week){
        for (Short tmp:weekList){
            if (((Short)week).equals(tmp)) return true;
        }
        return false;
    }

    public static String weekListToString(Set<Short> weekList){
        StringBuilder s= new StringBuilder();
        Object[] obj=weekList.toArray();
        int l=weekList.size()-1;
        for (int i=0;i<l;i++){
            s.append((obj[i])).append(",");
        }
        s.append(obj[l]);
        return s.toString();
    }

    //course table
    //获得该日期在第几周，-1表示该日期不在该学期内
    public static short getWeek(Date date, Date begin, Date end){
        long beginTime=begin.getTime();
        long endTime=end.getTime();
        long dateTime=date.getTime();
        if (beginTime<=dateTime && dateTime<=endTime){
            long daysBetween=(dateTime-beginTime)/(1000*3600*24);
            return (short)((daysBetween/7)+1);
        }else return -1;//传入日期不合法
    }

    //test -> to throw entityNotFoundException
    public static void testCourse(String courseId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select *from course where id=?;")){
            stmt.setString(1,courseId);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                throw new EntityNotFoundException();
            }
            rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void testSemester(int semesterId){
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select *from semester where id=?;")){
            stmt.setInt(1,semesterId);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                throw new EntityNotFoundException();
            }
            rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void testSection(int sectionId){
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select *from course_section where id=?;")){
            stmt.setInt(1,sectionId);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                throw new EntityNotFoundException();
            }
            rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void testClass(int classId){
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select *from course_section_class where id=?;")){
            stmt.setInt(1,classId);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                throw new EntityNotFoundException();
            }
            rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

    public static void testStudent(int studentId){
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select *from student where id=?;")){
            stmt.setInt(1,studentId);
            ResultSet rs=stmt.executeQuery();
            if (!rs.next()){
                rs.close();
                throw new EntityNotFoundException();
            }
            rs.close();
        }catch (SQLException e){
            e.printStackTrace();
        }
    }

}
