package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.CourseService;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class ReferenceCourseService implements CourseService {
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();

    @Override
    public void addCourse(String courseId, String courseName, int credit, int classHour, Course.CourseGrading grading, @Nullable Prerequisite prerequisite) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into course \n" +
                    "(id, name, credit, class_hour, course_grading) values ( ? , ? , ? , ? , ? );");
            PreparedStatement stmtPrerequisite=connection.prepareStatement("insert into prerequisite \n" +
                    "(course_id, value, children) values ( ? , ? , ? );");
            PreparedStatement stmtGetNID=connection.prepareStatement("select node_id from prerequisite \n" +
                    "where course_id= ? and value= ? and children= ?;")){
            stmt.setString(1,courseId);
            stmt.setString(2,courseName);
            stmt.setInt(3,credit);
            stmt.setInt(4,classHour);
            stmt.setString(5,Function.getGradingByCourseGrading(grading));
            stmtPrerequisite.setString(1,courseId);
            stmtGetNID.setString(1,courseId);
            stmt.executeUpdate();
            addCourseDFS(prerequisite,stmtPrerequisite,stmtGetNID);
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    private int addCourseDFS(Prerequisite prerequisite,PreparedStatement stmt,PreparedStatement stmtGetNID) throws SQLException{
        if (prerequisite==null) {
            stmt.setString(2,"none");
            stmt.setString(3,"-2");
            stmt.executeUpdate();
            stmtGetNID.setString(2,"none");
            stmtGetNID.setString(3,"-2");
            ResultSet rs=stmtGetNID.executeQuery();
            int nid=0;
            if (rs.next()){
                nid=rs.getInt(1);
            }
            rs.close();
            return nid;//nodeId
        }
        String thisType=prerequisite.when(new Prerequisite.Cases<String>() {
            @Override
            public String match(AndPrerequisite self) {
                return "AND";
            }
            @Override
            public String match(OrPrerequisite self) {
                return "OR";
            }
            @Override
            public String match(CoursePrerequisite self) {
                return self.courseID;
            }
        });
        if (thisType.equals("AND")){
            AndPrerequisite and=(AndPrerequisite) prerequisite;
            int size=and.terms.size();
            StringBuilder array= new StringBuilder();
            for (int i=0;i<size-1;i++){
                array.append(addCourseDFS(and.terms.get(i), stmt, stmtGetNID)).append(",");
            }
            array.append(addCourseDFS(and.terms.get(size - 1), stmt, stmtGetNID));
            stmt.setString(2,"and");
            stmt.setString(3, array.toString());
            stmt.executeUpdate();
            stmtGetNID.setString(2,"and");
            stmtGetNID.setString(3, array.toString());
            ResultSet rs=stmtGetNID.executeQuery();
            int nid=0;
            if (rs.next()){
                nid=rs.getInt(1);
            }
            rs.close();
            return nid;//nodeId
        }else if (thisType.equals("OR")){
            OrPrerequisite or=(OrPrerequisite) prerequisite;
            int size=or.terms.size();
            StringBuilder array=new StringBuilder();
            for (int i=0;i<size-1;i++){
                array.append(addCourseDFS(or.terms.get(i),stmt,stmtGetNID)).append(",");
            }
            array.append(addCourseDFS(or.terms.get(size-1),stmt,stmtGetNID));
            stmt.setString(2,"or");
            stmt.setString(3,array.toString());
            stmt.executeUpdate();
            stmtGetNID.setString(2,"or");
            stmtGetNID.setString(3,array.toString());
            ResultSet rs=stmtGetNID.executeQuery();
            int nid=0;
            if (rs.next()){
                nid=rs.getInt(1);
            }
            rs.close();
            return nid;//nodeId
        }else {
            CoursePrerequisite course=(CoursePrerequisite) prerequisite;
            stmt.setString(2,course.courseID);
            stmt.setString(3,"-1");
            stmt.executeUpdate();
            stmtGetNID.setString(2,course.courseID);
            stmtGetNID.setString(3,"-1");
            ResultSet rs=stmtGetNID.executeQuery();
            int nid=0;
            if (rs.next()){
                nid=rs.getInt(1);
            }
            rs.close();
            return nid;//nodeId
        }
    }

    @Override
    public int addCourseSection(String courseId, int semesterId, String sectionName, int totalCapacity) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into course_section " +
                    "    (name, total_capacity, left_capacity, course_id, semester_id) " +
                    "    values ( ? , ? , ? , ? , ? );");
            PreparedStatement stmtGet=connection.prepareStatement("select * from course_section " +
                    "where course_id = ? and semester_id = ? and name = ? ;")){
            stmt.setString(1,sectionName);
            stmt.setInt(2,totalCapacity);
            stmt.setInt(3,totalCapacity);
            stmt.setString(4,courseId);
            stmt.setInt(5,semesterId);
            stmt.executeUpdate();
            stmtGet.setString(1,courseId);
            stmtGet.setInt(2,semesterId);
            stmtGet.setString(3,sectionName);
            ResultSet rs=stmtGet.executeQuery();
            int id=0;
            if (rs.next()){
                id=rs.getInt(1);
            }
            rs.close();
            return id;
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    @Override
    public int addCourseSectionClass(int sectionId, int instructorId, DayOfWeek dayOfWeek, Set<Short> weekList, short classStart, short classEnd, String location) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into course_section_class " +
                    "(instructor_id, day_of_week, week_list, class_begin, class_end, location, course_section_id) " +
                    "values ( ? , ? , ? , ? , ? , ? , ? );");
            PreparedStatement stmtGet=connection.prepareStatement("select * from course_section_class " +
                    "where instructor_id = ? and day_of_week = ? " +
                    "and week_list = ? and class_begin = ? and class_end = ? " +
                    "and location = ? and course_section_id = ? ;")){
            if (classStart>classEnd){
                throw integrityViolationException;
            }
            stmt.setInt(1,instructorId);
            String day=Function.dayOfWeekToString(dayOfWeek);
            stmt.setString(2,day);
            String week=Function.weekListToString(weekList);
            stmt.setString(3,week);
            stmt.setInt(4,classStart);
            stmt.setInt(5,classEnd);
            stmt.setString(6,location);
            stmt.setInt(7,sectionId);
            stmt.executeUpdate();
            stmtGet.setInt(1,instructorId);
            stmtGet.setString(2,day);
            stmtGet.setString(3,week);
            stmtGet.setInt(4,classStart);
            stmtGet.setInt(5,classEnd);
            stmtGet.setString(6,location);
            stmtGet.setInt(7,sectionId);
            ResultSet rs=stmtGet.executeQuery();
            int id=0;
            if (rs.next()){
                id=rs.getInt(1);
            }
            rs.close();
            return id;
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    @Override
    public void removeCourse(String courseId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGetCourse =connection.prepareStatement("select id from course_section where course_id= ? ;");
            PreparedStatement stmtCourse = connection.prepareStatement("delete from course where id = ?;");
            PreparedStatement stmtMajorCourse=connection.prepareStatement("delete from major_course where course_id = ?;");
            PreparedStatement stmtPrerequisite =connection.prepareStatement("delete from prerequisite where course_id = ?;");
            PreparedStatement stmtGet=connection.prepareStatement("select *from course where id= ?;")) {
            stmtGet.setString(1,courseId);
            ResultSet rsGet=stmtGet.executeQuery();
            if (rsGet.next()) {
                stmtGetCourse.setString(1, courseId);
                stmtCourse.setString(1, courseId);
                stmtMajorCourse.setString(1, courseId);
                stmtPrerequisite.setString(1, courseId);
                ResultSet rs = stmtGetCourse.executeQuery();
                List<Integer> sectionId = new ArrayList<>();
                while (rs.next()) {
                    sectionId.add(rs.getInt(1));
                }
                rs.close();
                for (int i = 0; i < sectionId.size(); i++) {
                    removeCourseSection(sectionId.get(i));
                }
                stmtPrerequisite.executeUpdate();
                stmtMajorCourse.executeUpdate();
                stmtCourse.executeUpdate();
            }else {
                rsGet.close();
                throw entityNotFoundException;
            }
            rsGet.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeCourseSection(int sectionId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("delete from course_section where id = ?;");
            PreparedStatement stmtGet=connection.prepareStatement("select *from course_section where id= ?;");
            PreparedStatement inCourseSectionClass = connection.prepareStatement("delete from course_section_class where course_section_id = ?;");
            PreparedStatement inStudentCourseSection = connection.prepareStatement("delete from student_course_section where course_section_id = ?;")) {
            stmtGet.setInt(1,sectionId);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()) {
                stmt.setInt(1, sectionId);
                inCourseSectionClass.setInt(1, sectionId);
                inStudentCourseSection.setInt(1, sectionId);
                inCourseSectionClass.executeUpdate();
                inStudentCourseSection.executeUpdate();
                stmt.executeUpdate();
            }else {
                rs.close();
                throw entityNotFoundException;
            }
            rs.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeCourseSectionClass(int classId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("delete from course_section_class where id = ?;");
            PreparedStatement stmtGet=connection.prepareStatement("select *from course_section_class where id= ?;")) {
            stmtGet.setInt(1,classId);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()) {
                stmt.setInt(1, classId);
                stmt.executeUpdate();
            }else {
                rs.close();
                throw entityNotFoundException;
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Course> getAllCourses() {
        List<Course> courseList=new ArrayList<>();
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select * from course;")){
            ResultSet rs=stmt.executeQuery();
            while (rs.next()){
                Course course=new Course();
                course.id=rs.getString(1);
                course.name=rs.getString(2);
                course.credit=rs.getInt(3);
                course.classHour=rs.getInt(4);
                String grading=rs.getString(5);
                course.grading=Function.getGradingByString(grading);
                courseList.add(course);
            }
            rs.close();
            return courseList;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return courseList;
    }

    @Override
    public List<CourseSection> getCourseSectionsInSemester(String courseId, int semesterId) {
        Function.testCourse(courseId);
        Function.testSemester(semesterId);
        List<CourseSection> sectionList=new ArrayList<>();
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select * from " +
                    "course_section where semester_id= ? and course_id= ? ;")){
            stmt.setInt(1,semesterId);
            stmt.setString(2,courseId);
            ResultSet rs=stmt.executeQuery();
            while (rs.next()){
                CourseSection section=new CourseSection();
                section.id=rs.getInt(1);
                section.name=rs.getString(2);
                section.totalCapacity=rs.getInt(3);
                section.leftCapacity=rs.getInt(4);
                sectionList.add(section);
            }
            rs.close();
            return sectionList;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return sectionList;
    }

    @Override
    public Course getCourseBySection(int sectionId) {
        //要求中的两种情况不需要分开讨论
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select c.id,c.name,c.credit,c.class_hour,c.course_grading " +
                    "from course c join course_section cs on c.id = cs.course_id where cs.id = ?;")){
            stmt.setInt(1,sectionId);
            ResultSet rs=stmt.executeQuery();
            if (rs.next()){
                Course course=new Course();
                course.id=rs.getString(1);
                course.name=rs.getString(2);
                course.credit=rs.getInt(3);
                course.classHour=rs.getInt(4);
                String grading=rs.getString(5);
                course.grading=Function.getGradingByString(grading);
                rs.close();
                return course;
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<CourseSectionClass> getCourseSectionClasses(int sectionId) {
        Function.testSection(sectionId);
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select * from course_section_class csc " +
                    "join instructor i on csc.instructor_id = i.id " +
                    "where course_section_id = ? ;")){
            stmt.setInt(1,sectionId);
            ResultSet rs=stmt.executeQuery();
            List<CourseSectionClass> sectionClassList=new ArrayList<>();
            while (rs.next()){
                CourseSectionClass sectionClass=new CourseSectionClass();
                sectionClass.id=rs.getInt(1);
                String dayOfWeek=rs.getString(3);
                sectionClass.dayOfWeek=Function.getDayOfWeek(dayOfWeek);
                String weekList=rs.getString(4);
                sectionClass.weekList=Function.getWeekList(weekList);
                sectionClass.classBegin=rs.getShort(5);
                sectionClass.classEnd=rs.getShort(6);
                sectionClass.location=rs.getString(7);
                Instructor instructor=new Instructor();
                instructor.id=rs.getInt(9);
                String firstName=rs.getString(10);
                String lastName=rs.getString(11);
                instructor.fullName=Function.getFullName(firstName,lastName);
                sectionClass.instructor=instructor;
                sectionClassList.add(sectionClass);
            }
            rs.close();
            return sectionClassList;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    //这里如果搜不到section是否需要抛出entityNotFound
    @Override
    public CourseSection getCourseSectionByClass(int classId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select cs.id,cs.name, " +
                    "cs.total_capacity,cs.left_capacity " +
                    "from course_section cs join course_section_class csc " +
                    "on cs.id = csc.course_section_id where csc.id = ? ;")){
            stmt.setInt(1,classId);
            ResultSet rs=stmt.executeQuery();
            if (rs.next()){
                CourseSection section=new CourseSection();
                section.id=rs.getInt(1);
                section.name=rs.getString(2);
                section.totalCapacity=rs.getInt(3);
                section.leftCapacity=rs.getInt(4);
                rs.close();
                return section;
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public List<Student> getEnrolledStudentsInSemester(String courseId, int semesterId) {
        Function.testCourse(courseId);
        Function.testSemester(semesterId);
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select s.id,s.first_name,s.last_name,s.enrolled_date," +
                    "       m.id,m.name,d.id,d.name from student_course_section scs " +
                    "    join student s on scs.student_id = s.id " +
                    "    join course_section cs on scs.course_section_id = cs.id " +
                    "    join major m on s.major_id = m.id " +
                    "    join department d on d.id = m.department_id " +
                    "where cs.course_id = ? and cs.semester_id = ? ;")){
            stmt.setString(1,courseId);
            stmt.setInt(2,semesterId);
            ResultSet rs=stmt.executeQuery();
            List<Student> studentList=new ArrayList<>();
            while (rs.next()){
                Student student=new Student();
                student.id=rs.getInt(1);
                String firstName=rs.getString(2);
                String lastName=rs.getString(3);
                student.fullName=Function.getFullName(firstName,lastName);
                student.enrolledDate=rs.getDate(4);
                Major major=new Major();
                major.id=rs.getInt(5);
                major.name=rs.getString(6);
                Department department=new Department();
                department.id=rs.getInt(7);
                department.name=rs.getString(8);
                major.department=department;
                student.major=major;
                studentList.add(student);
            }
            rs.close();
            return studentList;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }
}