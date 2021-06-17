package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.StudentService;

import javax.annotation.Nullable;
import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.util.*;

import static cn.edu.sustech.cs307.dto.grade.PassOrFailGrade.FAIL;
import static cn.edu.sustech.cs307.dto.grade.PassOrFailGrade.PASS;

public class ReferenceStudentService implements StudentService {
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();

    @Override
    public void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into student " +
                    "(id, first_name, last_name, enrolled_date, major_id) " +
                    "values ( ? , ? , ? , ? , ? );")){
            stmt.setInt(1,userId);
            stmt.setString(2,firstName);
            stmt.setString(3,lastName);
            stmt.setDate(4,enrolledDate);
            stmt.setInt(5,majorId);
            stmt.executeUpdate();
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    public synchronized List<CourseSearchEntry> searchCourse(int studentId, int semesterId, @Nullable String searchCid,
                                                @Nullable String searchName, @Nullable String searchInstructor,
                                                @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime,
                                                @Nullable List<String> searchClassLocations, CourseType searchCourseType,
                                                boolean ignoreFull, boolean ignoreConflict, boolean ignorePassed,
                                                boolean ignoreMissingPrerequisites, int pageSize, int pageIndex) {
        List<Course> qCourse=new LinkedList<>();
        List<CourseSection> qSection=new LinkedList<>();
        List<CourseSectionClass> qClass=new LinkedList<>();

        int maxPrevSize=0;
        int cnt=0;
        List<Course> courseList = new ArrayList<>();
        List<CourseSection> sectionList = new ArrayList<>();
        List<CourseSectionClass> classList = new ArrayList<>();
        List<String> passCourse = new ArrayList<>();
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("select *from search_course_view where semester_id= ?;");
             PreparedStatement stmtPass = connection.prepareStatement("select cs.course_id,grade from student_course_section scs\n" +
                     "join course_section cs on cs.id = scs.course_section_id\n" +
                     "where student_id= ?;");
             PreparedStatement stmtConflict = connection.prepareStatement("select c.id,c.name,\n" +
                     "       cs.id,cs.name,\n" +
                     "       csc.id,csc.day_of_week,csc.week_list,csc.class_begin,csc.class_end\n" +
                     "from student_course_section scs\n" +
                     "    join course_section cs on cs.id = scs.course_section_id\n" +
                     "    left join course_section_class csc on cs.id = csc.course_section_id\n" +
                     "    join course c on c.id = cs.course_id\n" +
                     "where student_id= ? and semester_id= ? order by c.name||'['||cs.name||']';");
             PreparedStatement stmtPrev=connection.prepareStatement("select count(*)cnt from course_section cs\n" +
                     "    join course_section_class csc on cs.id = csc.course_section_id\n" +
                     "group by cs.id order by cnt desc limit 1;")) {
            ResultSet rs=stmtPrev.executeQuery();
            if (rs.next()){
                maxPrevSize=rs.getInt(1);
            }
            stmt.setInt(1, semesterId);
            stmtPass.setInt(1, studentId);
            rs = stmt.executeQuery();
            while (rs.next()) {
                Course course = new Course();
                CourseSection section = new CourseSection();
                CourseSectionClass sectionClass = new CourseSectionClass();
                course.id = rs.getString(1);
                course.name = rs.getString(2);
                course.credit = rs.getInt(3);
                course.classHour = rs.getInt(4);
                course.grading = Function.getGradingByString(rs.getString(5));
                section.id = rs.getInt(6);
                section.name = rs.getString(7);
                section.totalCapacity = rs.getInt(8);
                section.leftCapacity = rs.getInt(9);
                sectionClass.instructor = new Instructor();
                sectionClass.instructor.id = rs.getInt(10);
                String instructorFirstName = rs.getString(11);
                String instructorLastName = rs.getString(12);
                sectionClass.instructor.fullName = Function.getFullName(instructorFirstName, instructorLastName);
                sectionClass.id = rs.getInt(13);
                sectionClass.dayOfWeek = Function.getDayOfWeek(rs.getString(14));
                sectionClass.weekList = Function.getWeekList(rs.getString(15));
                sectionClass.classBegin = rs.getShort(16);
                sectionClass.classEnd = rs.getShort(17);
                sectionClass.location = rs.getString(18);
                if (cnt>0){
                    if (sectionList.get(cnt-1).id==section.id){//同一个section不同的class
                        courseList.add(course);
                        sectionList.add(section);
                        classList.add(sectionClass);
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        cnt++;
                        continue;
                    }
                }
                if (searchCid != null) {
                    if (!course.id.contains(searchCid)) {
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                String fullName = String.format("%s[%s]", course.name, section.name);
                if (searchName != null) {
                    if (!fullName.contains(searchName)) {
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                if (searchInstructor != null) {
                    if (!((instructorFirstName.indexOf(searchInstructor) == 0)
                            || (instructorLastName.indexOf(searchInstructor) == 0)
                            || (sectionClass.instructor.fullName.indexOf(searchInstructor) == 0)
                            || (String.format("%s%s",instructorFirstName,instructorLastName).indexOf(searchInstructor)==0))){
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                if (searchDayOfWeek != null) {
                    if (!sectionClass.dayOfWeek.equals(searchDayOfWeek)) {
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                if (searchClassTime != null) {
                    if (!(sectionClass.classBegin <= searchClassTime && searchClassTime <= sectionClass.classEnd)){
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                if (searchClassLocations != null) {
                    int flag = 0;
                    for (String searchClassLocation : searchClassLocations) {
                        if (searchClassLocation.length() > 0 && sectionClass.location.contains(searchClassLocation)) {//只要有相等的，flag=1
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 0) {
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;//无匹配项
                    }
                }
                if (ignoreFull) {//删去剩余容量==0
                    if (section.leftCapacity == 0){
                        if (qCourse.size()==maxPrevSize){
                            qCourse.remove(0);
                            qSection.remove(0);
                            qClass.remove(0);
                        }
                        qCourse.add(course);
                        qSection.add(section);
                        qClass.add(sectionClass);
                        continue;
                    }
                }
                for (int i=qSection.size()-1;i>=0;i--){
                    if (qSection.get(i).id==section.id){
                        courseList.add(qCourse.get(i));
                        sectionList.add(qSection.get(i));
                        classList.add(qClass.get(i));
                        cnt++;
                    }else break;
                }
                courseList.add(course);
                sectionList.add(section);
                classList.add(sectionClass);
                qCourse=new ArrayList<>();
                qSection=new ArrayList<>();
                qClass=new ArrayList<>();
                cnt++;
            }
            rs = stmtPass.executeQuery();
            while (rs.next()) {
                String cid = rs.getString(1);
                String grade = rs.getString(2);
                if (pass(grade)) {
                    passCourse.add(cid);
                }
            }
            rs.close();
            int holeListSize = courseList.size();
            List<Course> isCourse = new ArrayList<>();
            List<CourseSection> isSection = new ArrayList<>();
            List<CourseSectionClass> intersection = new ArrayList<>();
            List<String> courseType = getCourseIdFromCourseType(studentId, searchCourseType);
            if (searchCourseType.equals(CourseType.ALL)) {
                isCourse = courseList;
                isSection = sectionList;
                intersection = classList;
            }
            for (int i = 0; i < holeListSize; i++) {
                int publicFlag=0;
                for (int j = 0; j < courseType.size(); j++) {
                    if (courseList.get(i).id.equals(courseType.get(j))) {
                        if (searchCourseType.equals(CourseType.MAJOR_COMPULSORY) ||
                                searchCourseType.equals(CourseType.MAJOR_ELECTIVE) ||
                                searchCourseType.equals(CourseType.CROSS_MAJOR)) {
                            isCourse.add(courseList.get(i));
                            isSection.add(sectionList.get(i));
                            intersection.add(classList.get(i));
                            break;
                        }
                    } else publicFlag++;
                    if (searchCourseType.equals(CourseType.PUBLIC) && publicFlag==courseType.size()) {
                        isCourse.add(courseList.get(i));
                        isSection.add(sectionList.get(i));
                        intersection.add(classList.get(i));
                    }
                }
            }
            if (ignorePassed) {//已通过的课
                int size = isSection.size();
                for (int i = 0, j = 0; i < size; i++, j++) {
                    for (String s : passCourse) {
                        if (s.equals(isCourse.get(j).id)) {
                            isSection.remove(j);
                            isCourse.remove(j);
                            intersection.remove(j);
                            j--;
                            break;
                        }
                    }
                }
            }
            if (ignoreMissingPrerequisites) {//删去先修课不满足条件的
                int size = isSection.size();
                for (int i = 0, j = 0; i < size; i++, j++) {
                    if (!passedPrerequisitesForCourseWithoutCheck(studentId, isCourse.get(j).id)) {//没通过先修课
                        isSection.remove(j);
                        isCourse.remove(j);
                        intersection.remove(j);
                        j--;
                    }
                }
            }
            List<Course> conflictCourse = new ArrayList<>();
            List<CourseSection> conflictSection = new ArrayList<>();
            List<CourseSectionClass> conflictClass = new ArrayList<>();
            //获得所有本学期已选的课程信息，用于比较结果集的冲突
            stmtConflict.setInt(1, studentId);
            stmtConflict.setInt(2, semesterId);
            ResultSet rs1 = stmtConflict.executeQuery();
            while (rs1.next()) {
                Course course = new Course();
                course.id = rs1.getString(1);
                course.name = rs1.getString(2);
                conflictCourse.add(course);
                CourseSection section = new CourseSection();
                section.id = rs1.getInt(3);
                section.name = rs1.getString(4);
                conflictSection.add(section);
                CourseSectionClass sectionClass = new CourseSectionClass();
                sectionClass.id = rs1.getInt(5);
                String dayOfWeek=rs1.getString(6);
                if (dayOfWeek!=null) {
                    sectionClass.dayOfWeek = Function.getDayOfWeek(dayOfWeek);
                }
                String weekList=rs1.getString(7);
                if (weekList!=null){
                    sectionClass.weekList = Function.getWeekList(weekList);
                }
                sectionClass.classBegin = rs1.getShort(8);
                sectionClass.classEnd = rs1.getShort(9);
                conflictClass.add(sectionClass);
            }
            rs1.close();
            List<CourseSearchEntry> cseList = new ArrayList<>();
            CourseSectionClass cla = new CourseSectionClass();
            cla.id = -1;
            CourseSection sec = new CourseSection();
            sec.id = -1;
            intersection.add(cla);
            isSection.add(sec);
            isCourse.add(new Course());//整一个尾防止越界
            int conflictSize = conflictCourse.size();//取出conflict的size
            conflictClass.add(cla);
            conflictSection.add(sec);
            conflictCourse.add(new Course());//给所有conflict的加一个尾防止越界
            List<CourseSectionClass> list = new ArrayList<>();
            List<String> thisConflict = new ArrayList<>();
            int[] conflictFlag;
            int[] vis=new int[conflictSize];
            for (int i = 0; i < isSection.size() - 1; i++) {
                conflictFlag = new int[conflictSize];
                //对该class判定冲突
                for (int j = 0; j < conflictSize; j++) {
                    //判断课程冲突
                    if (isCourse.get(i).id.equals(conflictCourse.get(j).id)) {
                        conflictFlag[j] = 1;
                        continue;
                    }
                    //判断class的时间是否冲突
                    if (conflictClass.get(j).id!=0) {
                        if (checkTimeConflict(conflictClass.get(j), intersection.get(i))) {
                            conflictFlag[j] = 1;
                            //往前遍历完当前section的
                            int k = j - 1;
                            while (k >= 0 && conflictSection.get(k).id == conflictSection.get(j).id) {
                                conflictFlag[k] = 1;
                                k--;
                            }
                            //往后遍历完当前section的
                            k = j + 1;
                            while (conflictSection.get(k).id == conflictSection.get(j).id) {
                                conflictFlag[k] = 1;
                                k++;
                            }
                        }
                    }
                }
                for (int j = 0; j < conflictSize; j++) {
                    if (conflictSection.get(j).id != conflictSection.get(j + 1).id) {
                        if (conflictFlag[j] == 1) {//冲突
                            String conflict = String.format("%s[%s]", conflictCourse.get(j).name, conflictSection.get(j).name);
                            if (vis[j]==0) {
                                thisConflict.add(conflict);//添加冲突课程
                            }
                            vis[j] = 1;
                        }
                    }
                }
                if (ignoreConflict) {//判断是否添加这个class到表里边
                    int flagConflict = 0;
                    for (int j = 0; j < conflictSize; j++) {
                        if (conflictFlag[j] == 1) {//有冲突项
                            flagConflict = 1;
                            break;
                        }
                    }
                    if (flagConflict == 0) {
                        list.add(intersection.get(i));
                    }
                } else {
                    list.add(intersection.get(i));
                }
                if (isSection.get(i).id != isSection.get(i + 1).id) {
                    CourseSearchEntry cse = new CourseSearchEntry();
                    cse.course = isCourse.get(i);
                    cse.section = isSection.get(i);
                    cse.sectionClasses = new HashSet<>(list);
                    Collections.sort(thisConflict);
                    cse.conflictCourseNames = thisConflict;
                    if (ignoreConflict) {//判断是否添加这个entry到表里边
                        if (thisConflict.size() == 0) {//无冲突项
                            cseList.add(cse);
                        }
                    } else {//显示冲突课程
                        cseList.add(cse);//将该entry添加至最终结果
                    }
                    list = new ArrayList<>();
                    thisConflict = new ArrayList<>();
                    vis=new int[conflictSize];
                }
            }
            //翻页
            List<CourseSearchEntry> resultList = new ArrayList<>();
            int canPage = cseList.size() / pageSize;
            if (cseList.size() % pageSize != 0) {
                canPage = canPage + 1;
            }
            if (pageIndex + 1 > canPage || pageIndex < 0) {
                return resultList;
            } else {
                int start = pageSize * (pageIndex);
                int end = Math.min(pageSize * (pageIndex + 1) - 1, cseList.size() - 1);
                for (int p = start; p <= end; p++) {
                    resultList.add(cseList.get(p));
                }
                return resultList;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    //TYPE: course type -> courseId
    private List<String> getCourseIdFromCourseType(int studentId,CourseType searchCourseType){
        List<String> courseId=new ArrayList<>();
        if (searchCourseType.equals(CourseType.ALL)){
            return courseId;
        }
        String sqlGetMajorId="select major_id from student where id= ?;";
        String sql;
        int type=0;
        if (searchCourseType.equals(CourseType.MAJOR_COMPULSORY)){
            sql="select course_id from major_course where type=1 and major_id= ?;";
        }else if (searchCourseType.equals(CourseType.MAJOR_ELECTIVE)){
            sql="select course_id from major_course where type=2 and major_id= ?;";
        }else if (searchCourseType.equals(CourseType.CROSS_MAJOR)){
            sql="select course_id from major_course where major_id!= ?;";
        }else{//!PUBLIC -> ALL MAJOR
            sql="select distinct course_id from major_course;";
            type=1;
        }
        try (Connection connection=SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt=connection.prepareStatement(sql);
             PreparedStatement stmtGetMajorId=connection.prepareStatement(sqlGetMajorId)){
            stmtGetMajorId.setInt(1,studentId);
            ResultSet rsGet=stmtGetMajorId.executeQuery();
            if (type==1){//不需要major_id
                ResultSet rs=stmt.executeQuery();
                while (rs.next()){
                    courseId.add(rs.getString(1));
                }
                rs.close();
                return courseId;
            }
            if (rsGet.next()){
                int majorId=rsGet.getInt(1);
                stmt.setInt(1,majorId);
                ResultSet rs=stmt.executeQuery();
                while (rs.next()){
                    courseId.add(rs.getString(1));
                }
                rs.close();
                rsGet.close();
                return courseId;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return courseId;
    }

    @Override
    public EnrollResult enrollCourse(int studentId, int sectionId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into student_course_section (student_id, course_section_id, grade) values (?,?,?);");
            PreparedStatement findCourseSection = connection.prepareStatement("select * from course_section where id = ?;");
            PreparedStatement searchStudentSection = connection.prepareStatement("select * from student_course_section where student_id =?;");
            PreparedStatement searchGrade=connection.prepareStatement("select scs.grade\n" +
                    "from student_course_section scs\n" +
                    "join course_section cs on cs.id = scs.course_section_id\n" +
                    "where cs.course_id= ? and student_id= ?;");
            PreparedStatement getSectionInSemester=connection.prepareStatement("select cs.id\n" +
                    "from student_course_section scs \n" +
                    "join course_section cs on scs.course_section_id = cs.id\n" +
                    "join course_section_class csc on cs.id = csc.course_section_id\n" +
                    "where scs.student_id= ? and cs.semester_id= ?;");
            PreparedStatement courseConflict=connection.prepareStatement("select scs.grade \n" +
                    "from student_course_section scs\n" +
                    "join course_section cs on cs.id = scs.course_section_id\n" +
                    "where course_id= ? and student_id = ? and semester_id= ?;");
            PreparedStatement update=connection.prepareStatement("update course_section \n" +
                    "set left_capacity=left_capacity-1 where id= ?;")) {
            stmt.setInt(1,studentId);
            stmt.setInt(2,sectionId);
            stmt.setString(3,"NULL");
            ReferenceCourseService referenceCourseService = new ReferenceCourseService();
            findCourseSection.setInt(1,sectionId);
            ResultSet rs = findCourseSection.executeQuery();
            if(!rs.next()){
                rs.close();
                return EnrollResult.COURSE_NOT_FOUND;//1.
            }
            int leftCapacity=rs.getInt(4);//存leftCapacity,用于6.中的courseIsFull
            String courseId=rs.getString(5);//存courseId,用于3.4.
            int semesterId=rs.getInt(6);//用于6.
            searchStudentSection.setInt(1,studentId);
            rs = searchStudentSection.executeQuery();
            while(rs.next()){
                if(sectionId == rs.getInt(2)){
                    rs.close();
                    return EnrollResult.ALREADY_ENROLLED;//2.
                }
            }
            searchGrade.setString(1,courseId);
            searchGrade.setInt(2,studentId);
            rs=searchGrade.executeQuery();
            while (rs.next()){
                String grade=rs.getString(1);
                if (pass(grade)){
                    rs.close();
                    return EnrollResult.ALREADY_PASSED;//3.
                }
            }
            if (!passedPrerequisitesForCourseWithoutCheck(studentId,courseId)){
                rs.close();
                return EnrollResult.PREREQUISITES_NOT_FULFILLED;//4.
            }
            courseConflict.setString(1,courseId);
            courseConflict.setInt(2,studentId);
            courseConflict.setInt(3,semesterId);
            rs=courseConflict.executeQuery();
            if (rs.next()){
                rs.close();
                return EnrollResult.COURSE_CONFLICT_FOUND;//5.已经选过这个课
            }
            getSectionInSemester.setInt(1,studentId);
            getSectionInSemester.setInt(2,semesterId);
            rs=getSectionInSemester.executeQuery();
            List<Integer> sectionIdList=new ArrayList<>();
            while (rs.next()){
                sectionIdList.add(rs.getInt(1));
            }
            List<CourseSectionClass> sectionClassList=new ArrayList<>();
            for (int i=0;i<sectionIdList.size();i++){
                sectionClassList.addAll(referenceCourseService.getCourseSectionClasses(sectionIdList.get(i)));
            }
            List<CourseSectionClass> thisSectionClassList=referenceCourseService.getCourseSectionClasses(sectionId);
            for (int i=0;i<thisSectionClassList.size();i++){
                CourseSectionClass thisSectionClass=thisSectionClassList.get(i);
                for (int j=0;j<sectionClassList.size();j++){
                    CourseSectionClass sectionClass=sectionClassList.get(j);
                    if (checkTimeConflict(thisSectionClass,sectionClass)){
                        rs.close();
                        return EnrollResult.COURSE_CONFLICT_FOUND;//5.时间冲突
                    }
                }
            }
            if (leftCapacity==0){
                rs.close();
                return EnrollResult.COURSE_IS_FULL;
            }
            stmt.executeUpdate();//insert
            update.setInt(1,sectionId);
            update.executeUpdate();
            rs.close();
            return EnrollResult.SUCCESS;
        } catch (SQLException e) {
            //e.printStackTrace();
            return EnrollResult.UNKNOWN_ERROR;//???
        }
        //return EnrollResult.UNKNOWN_ERROR;//???
    }

    private boolean checkTimeConflict(CourseSectionClass class1,CourseSectionClass class2){
        Object[] list1=class1.weekList.toArray();
        Object[] list2=class2.weekList.toArray();
        for (int i=0;i<list1.length;i++){
            for (int j=0;j<list2.length;j++){
                if (list1[i].equals(list2[j])){
                    if (class1.dayOfWeek.equals(class2.dayOfWeek)){
                        if (!((class2.classEnd<class1.classBegin)||(class1.classEnd<class2.classBegin))){
                            return true;//conflict
                        }
                    }
                }
            }
        }
        return false;
    }

    private boolean pass(String grade){
        if (grade.equals("PASS")){
            return true;
        }else if (grade.equals("FAIL") || grade.equals("NULL")){
            return false;
        }else {//百分制
            int grd=Integer.parseInt(grade);
            return grd >= 60;
        }
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException {//退课
        Function.testStudent(studentId);
        Function.testSection(sectionId);
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGet = connection.prepareStatement("select grade from\n" +
                    "student_course_section where student_id= ? and course_section_id = ?;");
            PreparedStatement stmt = connection.prepareStatement("delete from student_course_section\n" +
                    "where student_id= ? and course_section_id= ? and grade='NULL';");
            PreparedStatement stmtUpdateSection=connection.prepareStatement("update course_section \n" +
                    "set left_capacity=left_capacity+1 where id= ?;")) {
            stmtGet.setInt(1,studentId);
            stmtGet.setInt(2,sectionId);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()){
                String grade=rs.getString(1);
                if (grade.equals("NULL")) {
                    stmt.setInt(1, studentId);
                    stmt.setInt(2, sectionId);
                    stmt.executeUpdate();//delete
                    stmtUpdateSection.setInt(1, sectionId);
                    stmtUpdateSection.executeUpdate();//update
                }else{
                    throw new IllegalStateException();
                }
            }else{
                rs.close();
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGet=connection.prepareStatement("select c.course_grading\n" +
                    "from course_section cs join course c on cs.course_id = c.id\n" +
                    "where cs.id= ? ;");
            PreparedStatement stmt=connection.prepareStatement("insert into student_course_section \n" +
                    "(student_id, course_section_id, grade) values ( ? , ? , ? );")){
            stmtGet.setInt(1,sectionId);
            stmt.setInt(1,studentId);
            stmt.setInt(2,sectionId);
            if (grade==null){
                stmt.setString(3,"NULL");
                stmt.executeUpdate();
                return;
            }
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()){
                String gradeType=rs.getString(1);
                String thisType=grade.when(new Grade.Cases<String>() {
                    @Override
                    public String match(PassOrFailGrade self) {
                        return self.name();
                    }
                    @Override
                    public String match(HundredMarkGrade self) {
                        return Short.toString(self.mark);
                    }
                });
                if (gradeType.equals("pf")){
                    if (thisType.equals("PASS") || thisType.equals("FAIL")){
                        stmt.setString(3,thisType);
                    }else {
                        rs.close();
                        throw integrityViolationException;
                    }
                }
                if (gradeType.equals("hm")){
                    if (thisType.equals("PASS") || thisType.equals("FAIL")){
                        rs.close();
                        throw integrityViolationException;
                    }else {
                        stmt.setString(3,thisType);
                    }
                }
                stmt.executeUpdate();
                rs.close();
            }else {
                rs.close();
                throw integrityViolationException;
            }
        }catch (SQLException e){
            e.printStackTrace();
            throw integrityViolationException;
        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGet=connection.prepareStatement("select c.course_grading \n" +
                    "from student_course_section scs\n" +
                    "    join course_section cs on scs.course_section_id = cs.id\n" +
                    "    join course c on cs.course_id = c.id\n" +
                    "where scs.student_id= ? and scs.course_section_id= ? ;");
            PreparedStatement stmt=connection.prepareStatement("update student_course_section set grade = ? \n" +
                    "where student_id = ? and course_section_id = ? ;")){
            stmtGet.setInt(1,studentId);
            stmtGet.setInt(2,sectionId);
            stmt.setInt(2,studentId);
            stmt.setInt(3,sectionId);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()){
                String gradeType=rs.getString(1);
                String thisType=grade.when(new Grade.Cases<String>() {
                    @Override
                    public String match(PassOrFailGrade self) {
                        return self.name();
                    }
                    @Override
                    public String match(HundredMarkGrade self) {
                        return Short.toString(self.mark);
                    }
                });
                if (gradeType.equals("pf")){
                    if (thisType.equals("PASS") || thisType.equals("FAIL")){
                        stmt.setString(1,thisType);
                    }else {
                        rs.close();
                        throw integrityViolationException;
                    }
                }
                if (gradeType.equals("hm")){
                    if (thisType.equals("PASS") || thisType.equals("FAIL")){
                        rs.close();
                        throw integrityViolationException;
                    }else {
                        stmt.setString(1,thisType);
                    }
                }
                stmt.executeUpdate();
                rs.close();
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        }catch (SQLException e){
            throw entityNotFoundException;
        }
    }

    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId) {
        Function.testStudent(studentId);
        if (semesterId!=null){
            Function.testSemester(semesterId);
        }
        Map<Course,Grade> map=new HashMap<>();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select scs.grade,s.begin,\n" +
                    "       c.id,c.name,c.credit,c.class_hour,c.course_grading\n" +
                    "from student_course_section scs\n" +
                    "join course_section cs on scs.course_section_id = cs.id\n" +
                    "join course c on cs.course_id = c.id\n" +
                    "join semester s on cs.semester_id = s.id\n" +
                    "where scs.student_id = ? and cs.semester_id = ? ;");
            PreparedStatement stmtNull=connection.prepareStatement("select scs.grade,s.begin,\n" +
                    "       c.id,c.name,c.credit,c.class_hour,c.course_grading\n" +
                    "from student_course_section scs\n" +
                    "join course_section cs on scs.course_section_id = cs.id\n" +
                    "join course c on cs.course_id = c.id\n" +
                    "join semester s on cs.semester_id = s.id\n" +
                    "where scs.student_id = ? ;")) {
            ResultSet rs;
            if (semesterId==null){
                stmtNull.setInt(1,studentId);
                rs=stmtNull.executeQuery();
                List<CourseGradeDate> list=new ArrayList<>();
                while (rs.next()){
                    String g=rs.getString(1);
                    Grade grade=changeGrade(g);
                    Date begin=rs.getDate(2);
                    Course course=new Course();
                    course.id=rs.getString(3);
                    course.name=rs.getString(4);
                    course.credit=rs.getInt(5);
                    course.classHour=rs.getInt(6);
                    course.grading=Function.getGradingByString(rs.getString(7));
                    int findFlag=0;
                    for (int i=0;i<list.size();i++){
                        CourseGradeDate cgdi=list.get(i);
                        if (cgdi.course.id.equals(course.id)){
                            findFlag=1;
                            if (cgdi.date.before(begin)){
                                map.put(course,grade);
                                break;
                            }
                        }
                    }
                    if (findFlag==0){
                        map.put(course,grade);
                    }
                    CourseGradeDate cgd=new CourseGradeDate(grade,course,begin);
                    list.add(cgd);
                }
                rs.close();
                return map;
            }else {
                stmt.setInt(1,studentId);
                stmt.setInt(2,semesterId);
                rs=stmt.executeQuery();
                //Map<Course,Grade> map=new HashMap<>();
                List<CourseGradeDate> list=new ArrayList<>();
                while (rs.next()){
                    String g=rs.getString(1);
                    Grade grade=changeGrade(g);
                    Date begin=rs.getDate(2);
                    Course course=new Course();
                    course.id=rs.getString(3);
                    course.name=rs.getString(4);
                    course.credit=rs.getInt(5);
                    course.classHour=rs.getInt(6);
                    course.grading=Function.getGradingByString(rs.getString(7));
                    int findFlag=0;
                    for (int i=0;i<list.size();i++){
                        CourseGradeDate cgdi=list.get(i);
                        if (cgdi.course.id.equals(course.id)){
                            findFlag=1;
                            if (cgdi.date.before(begin)){
                                map.put(course,grade);
                                break;
                            }
                        }
                    }
                    if (findFlag==0){
                        map.put(course,grade);
                    }
                    CourseGradeDate cgd=new CourseGradeDate(grade,course,begin);
                    list.add(cgd);
                }
                rs.close();
                return map;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return map;
    }

    private static class CourseGradeDate{
        Grade grade;
        Course course;
        Date date;
        public CourseGradeDate(Grade grade,Course course,Date date){
            this.grade=grade;
            this.course=course;
            this.date=date;
        }
    }

    private Grade changeGrade(String grade){
        if (grade.equals("NULL")){
            return null;
        }else if (grade.equals("PASS")){
            return PASS;
        }else if (grade.equals("FAIL")){
            return FAIL;
        }else {
            return new HundredMarkGrade(Short.parseShort(grade));
        }
    }

    @Override
    public CourseTable getCourseTable(int studentId, Date date) {
        Function.testStudent(studentId);
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select s.begin,s.\"end\",\n" +
                    "       c.name,cs.name,i.id,i.first_name,i.last_name,\n" +
                    "       csc.day_of_week,csc.week_list,csc.class_begin,csc.class_end,csc.location\n" +
                    "from semester s join course_section cs on s.id = cs.semester_id\n" +
                    "join course_section_class csc on cs.id = csc.course_section_id\n" +
                    "join course c on cs.course_id = c.id\n" +
                    "join instructor i on csc.instructor_id = i.id\n" +
                    "join student_course_section scs on cs.id = scs.course_section_id\n" +
                    "where scs.student_id = ? ;")){
            stmt.setInt(1,studentId);
            ResultSet rs=stmt.executeQuery();
            Map<DayOfWeek, Set<CourseTable.CourseTableEntry>> table=new HashMap<>();
            for (int i=1;i<=7;i++){
                Set<CourseTable.CourseTableEntry> list=new HashSet<>();
                table.put(Function.getDayOfWeekOfInt(i),list);
            }
            while (rs.next()){
                Date semesterBegin=rs.getDate(1);
                Date semesterEnd=rs.getDate(2);
                String courseName=rs.getString(3);
                String sectionName=rs.getString(4);
                int instructorId=rs.getInt(5);
                String instructorFirstName=rs.getString(6);
                String instructorLastName=rs.getString(7);
                DayOfWeek dayOfWeek=Function.getDayOfWeek(rs.getString(8));
                Set<Short> weekList=Function.getWeekList(rs.getString(9));
                short classBegin=rs.getShort(10);
                short classEnd=rs.getShort(11);
                String classLocation=rs.getString(12);
                short currentWeek=Function.getWeek(date,semesterBegin,semesterEnd);
                if (currentWeek!=-1){
                    if (Function.hasWeek(weekList,currentWeek)){
                        CourseTable.CourseTableEntry entry=new CourseTable.CourseTableEntry();
                        entry.courseFullName=String.format("%s[%s]",courseName,sectionName);
                        entry.instructor=new Instructor();
                        entry.instructor.id=instructorId;
                        entry.instructor.fullName=Function.getFullName(instructorFirstName,instructorLastName);
                        entry.classBegin=classBegin;
                        entry.classEnd=classEnd;
                        entry.location=classLocation;
                        table.get(dayOfWeek).add(entry);
                    }
                }
            }
            rs.close();
            CourseTable courseTable=new CourseTable();
            courseTable.table=table;
            return courseTable;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public boolean passedPrerequisitesForCourse(int studentId, String courseId){
        Function.testStudent(studentId);
        Function.testCourse(courseId);
        return passedPrerequisitesForCourseWithoutCheck(studentId,courseId);
    }

    private boolean passedPrerequisitesForCourseWithoutCheck(int studentId, String courseId){
        try (Connection connection= SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt=connection.prepareStatement("select scs.grade\n" +
                     "from student_course_section scs \n" +
                     "join course_section cs on scs.course_section_id = cs.id \n" +
                     "where cs.course_id= ? and scs.student_id= ?;")){
            Prerequisite prerequisite=getPrerequisite(courseId);
            stmt.setInt(2,studentId);
            return checkPassPrerequisiteDFS(stmt,prerequisite);
        }catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    private boolean checkPassPrerequisiteDFS(PreparedStatement stmt,Prerequisite node)throws SQLException{
        if (node==null){
            return true;
        }
        String thisType=node.when(new Prerequisite.Cases<String>() {
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
            AndPrerequisite and=(AndPrerequisite) node;
            int size=and.terms.size();
            int flag=0;
            for (int i=0;i<size;i++){
                if (checkPassPrerequisiteDFS(stmt,and.terms.get(i))){
                    flag++;
                }
            }
            return flag == size;
        }else if (thisType.equals("OR")){
            OrPrerequisite or=(OrPrerequisite) node;
            int size=or.terms.size();
            for (int i=0;i<size;i++){
                if (checkPassPrerequisiteDFS(stmt,or.terms.get(i))){
                    return true;
                }
            }
            return false;
        }else {
            stmt.setString(1,thisType);
            ResultSet rs=stmt.executeQuery();
            boolean pass=false;
            while (rs.next()){
                pass=pass(rs.getString(1));
            }
            rs.close();
            return pass;
        }
    }

    public Prerequisite getPrerequisite(String courseId){//通过courseId获得先修课的根节点
        try (Connection connection= SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmtGetRoot=connection.prepareStatement("select * from prerequisite \n" +
                     "where course_id= ? order by node_id desc limit 1;");
             PreparedStatement stmtDFS=connection.prepareStatement("select * from prerequisite where node_id= ? ;")){
            stmtGetRoot.setString(1,courseId);
            ResultSet rs=stmtGetRoot.executeQuery();
            Prerequisite prerequisite=null;
            if (rs.next()){
                int nodeId=rs.getInt(2);
                prerequisite=getPrerequisiteDFS(stmtDFS,nodeId);
            }
            rs.close();
            return prerequisite;
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }

    private Prerequisite getPrerequisiteDFS(PreparedStatement stmtDFS,int node)throws SQLException{
        stmtDFS.setInt(1,node);
        ResultSet rs=stmtDFS.executeQuery();
        if (rs.next()){
            String value=rs.getString(3);
            String child=rs.getString(4);
            List<Integer> childList=cast(child);
            int size=childList.size();
            if (value.equals("none")){
                return null;
            }else if (value.equals("and")){
                List<Prerequisite> prerequisiteList=new ArrayList<>();
                for (int i=0;i<size;i++){
                    prerequisiteList.add(getPrerequisiteDFS(stmtDFS,childList.get(i)));
                }
                return new AndPrerequisite(prerequisiteList);
            }else if (value.equals("or")){
                List<Prerequisite> prerequisiteList=new ArrayList<>();
                for (int i=0;i<size;i++){
                    prerequisiteList.add(getPrerequisiteDFS(stmtDFS,childList.get(i)));
                }
                return new OrPrerequisite(prerequisiteList);
            }else {
                return new CoursePrerequisite(value);
            }
        }
        rs.close();
        return null;
    }

    private List<Integer> cast(String value){
        String[] split = value.split(",");
        List<Integer> list=new ArrayList<>();
        for (int i=0;i<split.length;i++){
            list.add(Integer.parseInt(split[i]));
        }
        return list;
    }

    @Override
    public Major getStudentMajor(int studentId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select m.id,m.name,m.department_id,d.name " +
                    "from student s join major m on s.major_id = m.id " +
                    "join department d on m.department_id = d.id where s.id = ? ;")){
            stmt.setInt(1,studentId);
            ResultSet rs=stmt.executeQuery();
            if (rs.next()){
                Major major=new Major();
                major.id=rs.getInt(1);
                major.name=rs.getString(2);
                Department department=new Department();
                department.id=rs.getInt(3);
                department.name=rs.getString(4);
                major.department=department;
                rs.close();
                return major;
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        }catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }
}
