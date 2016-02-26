Documentation / initial impressions:

Unclear from help how adduser argument works. ADD_USERNAME suggests a username but help doc says its "their full name". 
Also, it's not clear if this can be used without uploading data or what..

General usability:

upload is the implicit action, but there seem to be other actions too. 
I'd suggest the first argument be the verb, e.g. ddsclient upload -p "project" dir1 dir2
I think that adding an explicit action as well as flags for projects (-p) or users (-u) 
will make the tool more understandable as additional functions are added (download, share, add_user).

Report formatting is a little tight and could use some whitespace in between the lines. 
As a next step, I think a structured report (e.g. JSON, XML, or YAML including the file checksums) 
would be great for archival or transformation into other formats.

Code-level

A LocalContent object is initialized with kind set to project. This seems important for accept to get started, please document this behavior in the initializer

RemoteContentSender and _send_different_local_content() seem pretty important, and could use more explanation
 (e.g. first we traverse the local content tree to get total file and folder counts/sizes. Second, ...)

At what point in the DDSClient.run() will a failed upload be caught? If a chunk fails, can it be retried automatically?
