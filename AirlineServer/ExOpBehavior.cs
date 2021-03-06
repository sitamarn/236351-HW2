﻿using System;
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

        public ExOpBehavior(CacheData cachedata)
        {
            cache = cachedata;
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
            dispatchOperation.Invoker = new ExOpInvoker(cache, dispatchOperation.Invoker);
        }

        public void Validate(OperationDescription operationDescription)
        {
            return;
        }
    }
}
