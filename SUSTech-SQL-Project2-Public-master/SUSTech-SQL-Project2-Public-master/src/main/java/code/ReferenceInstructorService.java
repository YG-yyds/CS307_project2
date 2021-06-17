package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.InstructorService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceInstructorService implements InstructorService {
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();

    @Override
    public void addInstructor(int userId, String firstName, String lastName) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("insert into instructor (id, first_name, last_name) values (?,?,?);")){
            stmt.setInt(1,userId);
            stmt.setString(2,firstName);
            stmt.setString(3,lastName);
            stmt.executeUpdate();
        }catch (SQLException e){
            throw integrityViolationException;
        }
    }

    @Override
    public List<CourseSection> getInstructedCourseSections(int instructorId, int semesterId) {
        try(Connection connection= SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt=connection.prepareStatement("select cs.id, cs.name, cs.total_capacity, " +
                    "cs.left_capacity, cs.course_id, cs.semester_id " +
                    "from course_section cs join course_section_class csc on cs.id = csc.course_section_id " +
                    "where cs.semester_id = ? and csc.instructor_id = ? ;")){
            stmt.setInt(1,semesterId);
            stmt.setInt(2,instructorId);
            ResultSet rs=stmt.executeQuery();
            List<CourseSection> sectionList=new ArrayList<>();
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
        return new ArrayList<>();
    }
}
