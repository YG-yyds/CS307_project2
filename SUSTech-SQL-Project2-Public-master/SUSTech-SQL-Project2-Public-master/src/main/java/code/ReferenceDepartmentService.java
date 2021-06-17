package code;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceDepartmentService implements DepartmentService {
    private IntegrityViolationException integrityViolationException=new IntegrityViolationException();
    private EntityNotFoundException entityNotFoundException=new EntityNotFoundException();

    @Override
    public int addDepartment(String name) {
        int id=0;
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into department(name) values (?);");
            PreparedStatement stmtGet=connection.prepareStatement("select id from department where name = ?;")) {
            List<Department> de = getAllDepartments();
            id=de.size();
            for(int i=0;i<id;i++){
                if (de.get(i).name.equals(name)) {
                    throw integrityViolationException;
                }
            }
            stmt.setString(1,name);
            stmt.executeUpdate();
            stmtGet.setString(1,name);
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

    @Override
    public void removeDepartment(int departmentId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmtGet = connection.prepareStatement("select id from major where department_id = ?;");
            PreparedStatement stmt = connection.prepareStatement("delete from department where id= ? ;");
            PreparedStatement stmtOut=connection.prepareStatement("select *from department where id= ?;")) {
            stmtOut.setInt(1,departmentId);
            ResultSet rsOut=stmtOut.executeQuery();
            if (rsOut.next()) {
                stmtGet.setInt(1, departmentId);
                ResultSet rs = stmtGet.executeQuery();
                List<Integer> majorId = new ArrayList<>();
                while (rs.next()) {
                    majorId.add(rs.getInt(1));
                }
                rs.close();
                ReferenceMajorService service = new ReferenceMajorService();
                for (int i = 0; i < majorId.size(); i++) {
                    service.removeMajor(majorId.get(i));
                }
                stmt.setInt(1, departmentId);
                stmt.executeUpdate();
            }else {
                rsOut.close();
                throw entityNotFoundException;
            }
            rsOut.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<Department> getAllDepartments() {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select*from department;")) {
            List<Department> departmentList = new ArrayList<>();
            ResultSet rs = stmt.executeQuery();
            while(rs.next()){
                Department department = new Department();
                department.id = rs.getInt(1);
                department.name = rs.getString(2);
                departmentList.add(department);
            }
            rs.close();
            return departmentList;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }

    @Override
    public Department getDepartment(int departmentId) {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("select*from department where id=?;")) {
            stmt.setInt(1,departmentId);
            ResultSet rs =stmt.executeQuery();
            if(rs.next()){
                Department department = new Department();
                department.id = rs.getInt(1);
                department.name = rs.getString(2);
                rs.close();
                return department;
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
