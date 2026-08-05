# Changes

Add support for `maxTrafficWeight`. 

This MAY change the behavior of rollouts with `maxTrafficWeight` already set, where all `setWeight` use values not more than 100. For instance, `setWeight: 10` with `maxTrafficWeight: 1000` will now route **1%** of the traffic, **NOT** 10%. If you use such settings, you should review the `setWeight` values.
