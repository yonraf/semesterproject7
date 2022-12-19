# github_data_pipeline

Install application and its dependencies

After installing the dependencies head over to the sound_stream dependency and dive into the SoundStreamPlugin.kt file.
In there you will find this function: onRequestPermissionsResult

Change the parameters from

override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>?, grantResults: IntArray?): Boolean {

to
override fun onRequestPermissionsResult(requestCode: Int, permissions: Array<out String>, grantResults: IntArray): Boolean {



Enable microphone on the android emulator in order to get audio stream to work. 