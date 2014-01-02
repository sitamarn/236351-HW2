using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Description;
using System.Text;
using System.Threading.Tasks;

namespace FlightSearchServer
{
    class ExOpBehavior : IOperationBehavior
    {
        string logFileName;

        public ExOpBehavior(string LogFileName)
        {
            logFileName = LogFileName;
        }
        public void AddBindingParameters(OperationDescription operationDescription, System.ServiceModel.Channels.BindingParameterCollection bindingParameters)
        {
            return;
        }

        public void ApplyClientBehavior(OperationDescription operationDescription, System.ServiceModel.Dispatcher.ClientOperation clientOperation)
        {
            return;
        }

        public void ApplyDispatchBehavior(OperationDescription operationDescription, System.ServiceModel.Dispatcher.DispatchOperation dispatchOperation)
        {
            dispatchOperation.Invoker = new ExOpInvoker(logFileName, dispatchOperation.Invoker);
        }

        public void Validate(OperationDescription operationDescription)
        {
            return;
        }
    }
}
