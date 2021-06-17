package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Instructor;
import cn.edu.sustech.cs307.dto.Student;
import cn.edu.sustech.cs307.dto.User;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceUserService implements UserService {
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();

    @Override
    public void removeUser(int userId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement instructor=connection.prepareStatement("select *from instructor where id= ?;");
            PreparedStatement stmtInstructor=connection.prepareStatement("delete from instructor where id= ?;");
            PreparedStatement stmtSectionClass=connection.prepareStatement("delete from course_section_class where instructor_id= ?;");
            PreparedStatement student=connection.prepareStatement("select *from student where id= ?;");
            PreparedStatement stmtStudent=connection.prepareStatement("delete from student where id= ?;");
            PreparedStatement stmtStudentSection=connection.prepareStatement("delete from student_course_section where student_id= ?;")) {
            int flag=0;
            instructor.setInt(1,userId);
            student.setInt(1,userId);
            ResultSet rs=instructor.executeQuery();
            if (rs.next()){
                stmtSectionClass.setInt(1,userId);
                stmtSectionClass.executeUpdate();
                stmtInstructor.setInt(1,userId);
                stmtInstructor.executeUpdate();
                flag=1;
            }
            rs=student.executeQuery();
            if (rs.next()){
                stmtStudentSection.setInt(1,userId);
                stmtStudentSection.executeUpdate();
                stmtStudent.setInt(1,userId);
                stmtStudent.executeUpdate();
                flag=1;
            }
            rs.close();
            if (flag==0){
                throw entityNotFoundException;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {//try括号中的资源会自动关闭
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtInstructor=connection.prepareStatement("select * from instructor;");
            PreparedStatement stmtStudent=connection.prepareStatement("select * from student;")){
            List<User> userList=new ArrayList<>();
            ResultSet rs=stmtInstructor.executeQuery();
            while (rs.next()){
                User user=new Instructor();
                user.id=rs.getInt(1);
                String first_name=rs.getString(2);
                String lase_name=rs.getString(3);
                user.fullName=Function.getFullName(first_name,lase_name);
                userList.add(user);
            }
            rs=stmtStudent.executeQuery();
            while (rs.next()){
                User user=new Student();
                user.id=rs.getInt(1);
                String first_name=rs.getString(2);
                String lase_name=rs.getString(3);
                user.fullName=Function.getFullName(first_name,lase_name);
                userList.add(user);
            }
            rs.close();
            return userList;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public User getUser(int userId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtInstructor=connection.prepareStatement("select *from instructor where id = ? ;");
            PreparedStatement stmtStudent=connection.prepareStatement("select *from student where id = ? ;")){
            stmtInstructor.setInt(1,userId);
            ResultSet rs=stmtInstructor.executeQuery();
            int flag1=0;
            int flag2=0;
            User user = null;
            if (rs.next()){
                user=new Instructor();
                user.id=rs.getInt(1);
                String first_name=rs.getString(2);
                String lase_name=rs.getString(3);
                user.fullName=Function.getFullName(first_name,lase_name);
                flag1=1;
            }else {
                stmtStudent.setInt(1,userId);
                rs=stmtStudent.executeQuery();
                if (rs.next()){
                    user=new Student();
                    user.id=rs.getInt(1);
                    String first_name=rs.getString(2);
                    String lase_name=rs.getString(3);
                    user.fullName=Function.getFullName(first_name,lase_name);
                    flag2=1;
                }
            }
            if (flag1==0 && flag2==0){
                throw entityNotFoundException;
            }
            rs.close();
            return user;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return null;
    }
}
