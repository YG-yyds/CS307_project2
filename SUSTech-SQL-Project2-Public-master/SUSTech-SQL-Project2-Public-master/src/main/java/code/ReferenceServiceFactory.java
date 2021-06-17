package code;

import cn.edu.sustech.cs307.factory.ServiceFactory;
import cn.edu.sustech.cs307.service.*;


public class ReferenceServiceFactory extends ServiceFactory {
    public ReferenceServiceFactory(){
        registerService(MajorService.class,new ReferenceMajorService());
        registerService(UserService.class,new ReferenceUserService());
        registerService(CourseService.class,new ReferenceCourseService());
        registerService(DepartmentService.class,new ReferenceDepartmentService());
        registerService(InstructorService.class,new ReferenceInstructorService());
        registerService(SemesterService.class,new ReferenceSemesterService());
        registerService(StudentService.class,new ReferenceStudentService());
    }
}
