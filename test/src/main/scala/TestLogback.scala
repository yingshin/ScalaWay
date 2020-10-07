import lombok.extern.slf4j.Slf4j
import org.slf4j.LoggerFactory

/**
 * Author: zhangying14
 * Date: 2020/9/27 19:36
 * Package: 
 * Description:
 *
 */

object TestLogback extends App {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)
  logger.error("this is an error log.")
  logger.info("this is an error log.")
  logger.warn("this is an error log.")
  logger.debug("this is an error log.")
  logger.trace("this is an error log.")
}
