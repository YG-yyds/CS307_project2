package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.MajorService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceMajorService implements MajorService {
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();

    @Override
    public int addMajor(String name, int departmentId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into major (name, department_id) values (?,?);");
            PreparedStatement stmtGet=connection.prepareStatement("select id from major\n" +
                    "where name = ? and department_id = ?;")){
            stmt.setString(1,name);
            stmt.setInt(2,departmentId);
            stmt.executeUpdate();
            stmtGet.setString(1,name);
            stmtGet.setInt(2,departmentId);
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
    public void removeMajor(int majorId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGetStudent = connection.prepareStatement("select id from student where major_id= ? ;");
            PreparedStatement stmtMajor= connection.prepareStatement("delete from major where id= ? ;");
            PreparedStatement stmtMajorCourse = connection.prepareStatement("delete from major_course where major_id= ? ;");
            PreparedStatement stmtStudent= connection.prepareStatement("delete from student where major_id= ? ;");
            PreparedStatement stmtStudentSection=connection.prepareStatement("delete from student_course_section where student_id= ? ;");
            PreparedStatement stmtGet=connection.prepareStatement("select *from major where id= ?;")) {
            stmtGet.setInt(1,majorId);
            ResultSet rsGet=stmtGet.executeQuery();
            if (rsGet.next()) {
                stmtGetStudent.setInt(1, majorId);
                ResultSet rs = stmtGetStudent.executeQuery();
                List<Integer> studentId = new ArrayList<>();
                while (rs.next()) {
                    studentId.add(rs.getInt(1));
                }
                rs.close();
                for (int i = 0; i < studentId.size(); i++) {
                    stmtStudentSection.setInt(1, studentId.get(i));
                    stmtStudentSection.executeUpdate();
                }
                stmtStudent.setInt(1, majorId);
                stmtStudent.executeUpdate();
                stmtMajorCourse.setInt(1, majorId);
                stmtMajorCourse.executeUpdate();
                stmtMajor.setInt(1, majorId);
                stmtMajor.executeUpdate();
            }else {
                rsGet.close();
                throw entityNotFoundException;
            }
            rsGet.close();
        } catch (SQLException e) {
            throw entityNotFoundException;
        }
    }

    @Override
    public List<Major> getAllMajors() {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select * from major m join department d on m.department_id = d.id;")) {
            ResultSet rs = stmt.executeQuery();
            List<Major> majorList=new ArrayList<>();
            while (rs.next()){
                Major major=new Major();
                major.id=rs.getInt(1);
                major.name=rs.getString(2);
                Department department=new Department();
                department.id=rs.getInt(3);
                department.name=rs.getString(5);
                major.department=department;
                majorList.add(major);
            }
            rs.close();
            return majorList;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public Major getMajor(int majorId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select * from major m join department d on m.department_id = d.id where m.id = ? ;")) {
            stmt.setInt(1,majorId);
            ResultSet rs = stmt.executeQuery();
            if (rs.next()){
                Major major=new Major();
                major.id=rs.getInt(1);
                major.name=rs.getString(2);
                Department department=new Department();
                department.id=rs.getInt(3);
                department.name=rs.getString(5);
                major.department=department;
                rs.close();
                return major;
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void addMajorCompulsoryCourse(int majorId, String courseId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into major_course (major_id, course_id, type) values (?,?,?);")){
            stmt.setInt(1,majorId);
            stmt.setString(2,courseId);
            stmt.setInt(3,1);//1: compulsory course
            stmt.executeUpdate();
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    @Override
    public void addMajorElectiveCourse(int majorId, String courseId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into major_course (major_id, course_id, type) values (?,?,?);")){
            stmt.setInt(1,majorId);
            stmt.setString(2,courseId);
            stmt.setInt(3,2);//2: elective course
            stmt.executeUpdate();
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }
}
