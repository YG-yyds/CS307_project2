import cn.edu.sustech.cs307.dto.CourseSearchEntry;
import cn.edu.sustech.cs307.dto.CourseSectionClass;
import cn.edu.sustech.cs307.dto.CourseTable;
import cn.edu.sustech.cs307.service.StudentService;
import code.*;
import java.sql.Date;

import java.time.DayOfWeek;
import java.util.*;

public class debug {
    public static void main(String[] args) {
        ReferenceStudentService studentService=new ReferenceStudentService();
        /*Date date=new Date(2019-1900,2-1,22);
        Date begin=new Date(2019-1900,2-1,1);
        Date end=new Date(2019-1900,7-1,3);
        CourseTable table =studentService.getCourseTable(11715173,date);
        System.out.println("week:"+Function.getWeek(date,begin,end));
        System.out.println("CourseTable:");
        for (int j=1;j<=7;j++){
            DayOfWeek dayOfWeek=Function.getDayOfWeekOfInt(j);
            System.out.println(dayOfWeek);
            Set<CourseTable.CourseTableEntry> entries=table.table.get(dayOfWeek);
            for (CourseTable.CourseTableEntry entry:entries){
                System.out.println(entry.courseFullName+" classBegin:"+entry.classBegin+" classEnd:"+entry.classEnd
                        +" instructor:"+entry.instructor.id+" "+entry.instructor.fullName+" location:"+entry.location);
            }
        }*/
        //System.out.println(studentService.enrollCourse(11712458,58));

        List<String> location=new ArrayList<>();
        location.add("荔园");
        List<CourseSearchEntry> entries=studentService.searchCourse(11710581,1,"C",null,
                null,null,null,null, StudentService.CourseType.ALL,
                true,false,true,true,10,0);
        System.out.println(entries.size());
        for (int j=0;j<entries.size();j++){
            CourseSearchEntry entry=entries.get(j);
            System.out.println("course:"+entry.course.id);
            System.out.println("section:"+entry.section.id+" "+entry.section.name);
            System.out.println("class:");
            for (CourseSectionClass cla:entry.sectionClasses){
                System.out.println(cla.id);
            }
            System.out.println("conflict:");
            for (int k=0;k<entry.conflictCourseNames.size();k++){
                System.out.println(entry.conflictCourseNames.get(k));
            }
        }



        //System.out.println(studentService.enrollCourse(11710780,3303));

        //test exception
        //ReferenceCourseService courseService=new ReferenceCourseService();
        //courseService.getCourseSectionsInSemester("ff",1);
    }
}
