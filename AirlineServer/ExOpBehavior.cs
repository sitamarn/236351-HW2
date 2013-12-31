using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel.Description;
using System.Text;
using System.Threading.Tasks;

namespace AirlineServer
{
    class ExOpBehavior : IOperationBehavior
    {
        CacheData cache = null;
        String filename = null;

        public ExOpBehavior(CacheData cachedata, string file)
        {
            cache = cachedata;
            filename = file;
        }
        public void AddBindingParameters(OperationDescription operationDescription, System.ServiceModel.Channels.BindingParameterCollection bindingParameters)
        {
            throw new NotImplementedException();
        }

        public void ApplyClientBehavior(OperationDescription operationDescription, System.ServiceModel.Dispatcher.ClientOperation clientOperation)
        {
            throw new NotImplementedException();
        }

        public void ApplyDispatchBehavior(OperationDescription operationDescription, System.ServiceModel.Dispatcher.DispatchOperation dispatchOperation)
        {
            //dispatchOperation.Invoker = new ExtensionOpInvoker(filename, cache, dispatchOperation.Invoker);
        }

        public void Validate(OperationDescription operationDescription)
        {
            throw new NotImplementedException();
        }
    }
}
