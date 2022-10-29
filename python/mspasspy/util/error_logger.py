from mspasspy.ccore.utility import ErrorLogger
import inspect


class PyErrorLogger(ErrorLogger):
    def msgErrorLog(self, message, err):
        callerFrame = inspect.stack()[2][0]
        methodName = callerFrame.f_code.co_name
        if "self" in callerFrame.f_locals:
            className = callerFrame.f_locals["self"].__class__.__name__
            methodName = className + "." + methodName
        return super().log_error(methodName, message, err)

    def log_error(self, *args, **kwargs):
        if len(args) == 1:
            return super().log_error(*args, **kwargs)
        elif len(args) == 2:  #   In this case, we want to use the new interface
            return self.msgErrorLog(*args, **kwargs)
        elif len(args) == 3:
            return super().log_error(*args, **kwargs)
        else:
            raise SyntaxError("log_error only takes 1/2/3 arguments")
