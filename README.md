Validation MVP to validate **source** replica sets prior to migration operations. 

### This validation will check the following:
- Is the index value a non-zero numeric value, hashed, text, or 2d.
- The value does not exceed the maximum allowed (1024 bytes).
- Are the options leveraged appropriate for the index type.
- Are the index options valid and recognized by MongoDB.

### Not Supported
- `oplog` validation. You will need to validate the size of the **source** `oplog` outside of this tool.

### Important:
Please kindly note, this is not an officially supported MongoDB product nor is it production ready at this time. This code is considered alpha. 
