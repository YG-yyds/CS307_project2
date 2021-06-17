package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Semester;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.SemesterService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class ReferenceSemesterService implements SemesterService {
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();
    @Override
    public int addSemester(String name, Date begin, Date end) {
        int id=0;
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("insert into semester(name,begin,\"end\") values (?,?,?);");
             PreparedStatement stmtGet = connection.prepareStatement("select id from semester \n" +
                     "where name = ? and begin = ? and \"end\" = ?;")) {
            List<Semester> se = getAllSemesters();
            id=se.size();
            for (int i=0;i<id;i++) {
                if (se.get(i).name.equals(name)) {
                    throw integrityViolationException;
                }
                if (!checkDate(se.get(i).begin,se.get(i).end,begin,end)){
                    throw integrityViolationException;
                }
            }
            if (begin.after(end) || begin.equals(end)){
                throw integrityViolationException;
            }
            stmt.setString(1, name);
            stmt.setDate(2, begin);
            stmt.setDate(3, end);
            stmt.executeUpdate();
            stmtGet.setString(1,name);
            stmtGet.setDate(2,begin);
            stmtGet.setDate(3,end);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()){
                id=rs.getInt(1);
            }else {
                id=0;
            }
            rs.close();
            return id;
        } catch (SQLException e) {
            throw integrityViolationException;
        }
    }

    private boolean checkDate(Date pBegin,Date pEnd,Date begin,Date end){
        if ((pBegin.before(begin) && pEnd.after(begin)) || (pBegin.before(end) && pEnd.after(end))){
            return false;
        }else return true;
    }

    @Override
    public void removeSemester(int semesterId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("delete from semester where id= ?;");
            PreparedStatement stmtSection=connection.prepareStatement("select id from course_section where semester_id= ?;");
            PreparedStatement stmtGet=connection.prepareStatement("select *from semester where id = ?;")) {
            stmtGet.setInt(1,semesterId);
            ResultSet rs=stmtGet.executeQuery();
            if (rs.next()) {
                stmtSection.setInt(1,semesterId);
                ResultSet rsIn=stmtSection.executeQuery();
                List<Integer> sectionId=new ArrayList<>();
                while (rsIn.next()){
                    sectionId.add(rsIn.getInt(1));
                }
                ReferenceCourseService service=new ReferenceCourseService();
                for (int i=0;i<sectionId.size();i++){
                    service.removeCourseSection(sectionId.get(i));
                }
                stmt.setInt(1,semesterId);
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
    public List<Semester> getAllSemesters() {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select*from semester;")) {
            List<Semester> semesterList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery();
            while (rs.next()){
                Semester semester = new Semester();
                semester.id = rs.getInt(1);
                semester.name = rs.getString(2);
                semester.begin = rs.getDate(3);
                semester.end = rs.getDate(4);
                semesterList.add(semester);
            }
            rs.close();
            return semesterList;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public Semester getSemester(int semesterId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select*from semester where id=?;")) {
            stmt.setInt(1,semesterId);
            ResultSet rs = stmt.executeQuery();
            if(rs.next()){
                Semester semester = new Semester();
                semester.id = rs.getInt(1);
                semester.name = rs.getString(2);
                semester.begin = rs.getDate(3);
                semester.end = rs.getDate(4);
                rs.close();
                return semester;
            }else {
                rs.close();
                throw entityNotFoundException;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }
}
