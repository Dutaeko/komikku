package eu.kanade.tachiyomi.extension.util

import android.app.ForegroundServiceStartNotAllowedException
import android.content.Context
import android.content.Intent
import android.os.Build
import androidx.core.content.ContextCompat
import androidx.core.net.toUri
import eu.kanade.domain.base.BasePreferences
import eu.kanade.tachiyomi.extension.installer.Installer
import eu.kanade.tachiyomi.extension.model.Extension
import eu.kanade.tachiyomi.extension.model.InstallStep
import eu.kanade.tachiyomi.network.NetworkHelper
import eu.kanade.tachiyomi.util.storage.getUriCompat
import eu.kanade.tachiyomi.util.system.isPackageInstalled
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.onCompletion
import kotlinx.coroutines.flow.transformWhile
import kotlinx.coroutines.launch
import logcat.LogPriority
import okhttp3.OkHttpClient
import okhttp3.Request
import tachiyomi.core.common.util.system.logcat
import uy.kohesive.injekt.Injekt
import uy.kohesive.injekt.api.get
import java.io.File
import java.io.IOException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * The installer which installs, updates and uninstalls the extensions.
 *
 * @param context The application context.
 */
internal class ExtensionInstaller(private val context: Context) {

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)
    private val idCounter = AtomicLong(System.currentTimeMillis())
    private val activeJobs = ConcurrentHashMap<String, Job>()
    private val activeIds = ConcurrentHashMap<String, Long>()
    private val activeSteps = ConcurrentHashMap<Long, MutableStateFlow<InstallStep>>()

    private val extensionInstaller = Injekt.get<BasePreferences>().extensionInstaller()
    private val httpClient: OkHttpClient = Injekt.get<NetworkHelper>().client

    /**
     * Adds the given extension to the downloads queue and returns an observable containing its
     * step in the installation process.
     *
     * @param url The url of the apk.
     * @param extension The extension to install.
     */
    fun downloadAndInstall(url: String, extension: Extension): Flow<InstallStep> {
        val pkgName = extension.installKey()
        cancelInstall(pkgName)
        val downloadId = idCounter.incrementAndGet()

        val step = MutableStateFlow(InstallStep.Pending)
        activeIds[pkgName] = downloadId
        activeSteps[downloadId] = step

        val tempFile = File(context.cacheDir, "extension_${pkgName.toSafeFileName()}.apk")
        val job = scope.launch {
            var installerStarted = false
            try {
                if (tempFile.exists() && !tempFile.delete()) {
                    throw IOException("Failed to delete previous extension APK")
                }
                step.value = InstallStep.Downloading
                val request = Request.Builder().url(url).build()
                httpClient.newCall(request).execute().use { response ->
                    if (!response.isSuccessful) {
                        throw IOException("Failed to download extension: HTTP ${response.code}")
                    }
                    val body = response.body ?: throw IOException("Downloaded extension body was empty")
                    tempFile.outputStream().use { output ->
                        body.byteStream().use { input ->
                            input.copyTo(output)
                        }
                    }
                }

                step.value = InstallStep.Installing
                installerStarted = installApk(downloadId, tempFile)
            } catch (e: CancellationException) {
                step.value = InstallStep.Idle
                throw e
            } catch (e: IOException) {
                logcat(LogPriority.ERROR, e) { "Failed to download extension ${extension.pkgName}" }
                step.value = InstallStep.Error
            } catch (e: SecurityException) {
                logcat(LogPriority.ERROR, e) { "Failed to install extension ${extension.pkgName}" }
                step.value = InstallStep.Error
            } catch (e: Exception) {
                logcat(LogPriority.ERROR, e) { "Failed to install extension ${extension.pkgName}" }
                step.value = InstallStep.Error
            } finally {
                if (!installerStarted && tempFile.exists()) {
                    tempFile.delete()
                }
            }
        }

        activeJobs[pkgName] = job

        return step.transformWhile {
            emit(it)
            !it.isCompleted()
        }.onCompletion {
            activeJobs.remove(pkgName, job)
            activeIds.remove(pkgName, downloadId)
            activeSteps.remove(downloadId, step)
            job.cancel()
        }
    }

    /**
     * Starts an intent to install the extension at the given file.
     *
     * @param tempFile The file of the extension to install.
     */
    private fun installApk(downloadId: Long, tempFile: File): Boolean {
        when (val installer = extensionInstaller.get()) {
            BasePreferences.ExtensionInstaller.LEGACY -> {
                return startLegacyInstaller(downloadId, tempFile)
            }
            BasePreferences.ExtensionInstaller.PRIVATE -> {
                try {
                    if (ExtensionLoader.installPrivateExtensionFile(context, tempFile)) {
                        updateInstallStep(downloadId, InstallStep.Installed)
                    } else {
                        updateInstallStep(downloadId, InstallStep.Error)
                    }
                } catch (e: Exception) {
                    logcat(LogPriority.ERROR, e) { "Failed to read downloaded extension file." }
                    updateInstallStep(downloadId, InstallStep.Error)
                }

                tempFile.delete()
                return true
            }
            else -> {
                val uri = tempFile.getUriCompat(context)
                val intent = ExtensionInstallService.getIntent(context, downloadId, uri, installer)
                return try {
                    ContextCompat.startForegroundService(context, intent)
                    true
                } catch (e: Exception) {
                    if (e.isForegroundServiceStartNotAllowed()) {
                        logcat(LogPriority.ERROR, e) { "Foreground service start not allowed for extension install" }
                    } else {
                        logcat(LogPriority.ERROR, e) { "Failed to start extension install service" }
                    }
                    startLegacyInstaller(downloadId, tempFile)
                }
            }
        }
    }

    private fun startLegacyInstaller(downloadId: Long, tempFile: File): Boolean {
        val intent = Intent(context, ExtensionInstallActivity::class.java)
            .setDataAndType(tempFile.getUriCompat(context), APK_MIME)
            .putExtra(EXTRA_DOWNLOAD_ID, downloadId)
            .setFlags(Intent.FLAG_ACTIVITY_NEW_TASK or Intent.FLAG_GRANT_READ_URI_PERMISSION)

        return try {
            context.startActivity(intent)
            true
        } catch (e: Exception) {
            logcat(LogPriority.ERROR, e) { "Failed to start legacy extension installer" }
            updateInstallStep(downloadId, InstallStep.Error)
            tempFile.delete()
            false
        }
    }

    /**
     * Cancels extension install and remove from installer.
     */
    fun cancelInstall(pkgName: String) {
        val downloadId = activeIds.remove(pkgName)
        activeJobs.remove(pkgName)?.cancel()
        if (downloadId != null) {
            Installer.cancelInstallQueue(context, downloadId)
        }
    }

    /**
     * Starts an intent to uninstall the extension by the given package name.
     *
     * @param pkgName The package name of the extension to uninstall
     */
    fun uninstallApk(pkgName: String) {
        if (context.isPackageInstalled(pkgName)) {
            @Suppress("DEPRECATION")
            val intent = Intent(Intent.ACTION_UNINSTALL_PACKAGE, "package:$pkgName".toUri())
                .setFlags(Intent.FLAG_ACTIVITY_NEW_TASK)
            context.startActivity(intent)
        } else {
            ExtensionLoader.uninstallPrivateExtension(context, pkgName)
            ExtensionInstallReceiver.notifyRemoved(context, pkgName)
        }
    }

    /**
     * Sets the step of the installation of an extension.
     *
     * @param downloadId The id of the download.
     * @param step New install step.
     */
    fun updateInstallStep(downloadId: Long, step: InstallStep) {
        activeSteps[downloadId]?.let { it.value = step }
    }

    private fun Exception.isForegroundServiceStartNotAllowed(): Boolean {
        return Build.VERSION.SDK_INT >= Build.VERSION_CODES.S && this is ForegroundServiceStartNotAllowedException
    }

    companion object {
        const val APK_MIME = "application/vnd.android.package-archive"
        const val EXTRA_DOWNLOAD_ID = "ExtensionInstaller.extra.DOWNLOAD_ID"

        fun Extension.installKey(): String = "${pkgName}_$signatureHash"

        private fun String.toSafeFileName(): String = replace(Regex("[^A-Za-z0-9._-]"), "_")
    }
}
