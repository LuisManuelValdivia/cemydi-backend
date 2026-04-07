import {
  Injectable,
  InternalServerErrorException,
  Logger,
} from '@nestjs/common';
import nodemailer, { type Transporter } from 'nodemailer';
import type SMTPTransport from 'nodemailer/lib/smtp-transport';
import dns from 'node:dns';
import net from 'node:net';
import tls from 'node:tls';

@Injectable()
export class MailService {
  private readonly logger = new Logger(MailService.name);
  private transporter: Transporter | null = null;

  private getTransporter() {
    if (this.transporter) {
      return this.transporter;
    }

    const host = process.env.SMTP_HOST?.trim();
    const port = Number(process.env.SMTP_PORT ?? '587');
    const user = process.env.SMTP_USER?.trim();
    const pass = process.env.SMTP_PASS?.trim();
    const secure =
      `${process.env.SMTP_SECURE ?? ''}`.trim().toLowerCase() === 'true';

    if (!host || !user || !pass || Number.isNaN(port)) {
      throw new InternalServerErrorException(
        'SMTP no esta configurado completamente. Define SMTP_HOST, SMTP_PORT, SMTP_USER y SMTP_PASS para enviar correos.',
      );
    }

    const transportOptions: SMTPTransport.Options = {
      host,
      port,
      secure,
      auth: {
        user,
        pass,
      },
      tls: {
        servername: host,
      },
      getSocket: (options, callback) => {
        dns.lookup(
          options.host ?? host,
          { family: 4 },
          (lookupError, address) => {
            if (lookupError) {
              callback(lookupError, false);
              return;
            }

            const socket = options.secure
              ? tls.connect({
                  host: address,
                  port: options.port ?? port,
                  servername: options.host ?? host,
                })
              : net.connect({
                  host: address,
                  port: options.port ?? port,
                });

            socket.once('error', (socketError) => callback(socketError, false));
            socket.once('connect', () =>
              callback(null, { connection: socket }),
            );
          },
        );
      },
    };

    this.transporter = nodemailer.createTransport(transportOptions);

    return this.transporter;
  }

  private getFromAddress() {
    return (
      process.env.MAIL_FROM?.trim() ||
      process.env.SMTP_USER?.trim() ||
      'no-reply@cemydi.local'
    );
  }

  async sendEmailVerificationLink(input: {
    correo: string;
    nombre: string;
    verificationUrl: string;
  }) {
    await this.sendMail({
      to: input.correo,
      subject: 'Verifica tu cuenta de CEMYDI',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.5;">
          <h2>Hola ${this.escapeHtml(input.nombre)}, confirma tu correo</h2>
          <p>Haz clic en el siguiente boton para verificar tu cuenta:</p>
          <p style="margin: 24px 0;">
            <a
              href="${input.verificationUrl}"
              style="display: inline-block; padding: 12px 20px; border-radius: 12px; background: #1e6260; color: #ffffff; text-decoration: none; font-weight: 700;"
            >
              Verificar mi cuenta
            </a>
          </p>
          <p>Si el boton no funciona, copia y pega este enlace en tu navegador:</p>
          <p><a href="${input.verificationUrl}">${input.verificationUrl}</a></p>
          <p>Si no solicitaste esta cuenta, puedes ignorar este mensaje.</p>
        </div>
      `,
      text: `Hola ${input.nombre}, verifica tu cuenta entrando a este enlace: ${input.verificationUrl}`,
    });
  }

  async sendPasswordResetCode(input: {
    correo: string;
    nombre: string;
    code: string;
  }) {
    await this.sendMail({
      to: input.correo,
      subject: 'Código para restablecer tu contraseña en CEMYDI',
      html: `
        <div style="font-family: Arial, sans-serif; line-height: 1.5;">
          <h2>Hola ${this.escapeHtml(input.nombre)}</h2>
          <p>Tu código para restablecer la contraseña es:</p>
          <p style="font-size: 28px; font-weight: 700; letter-spacing: 6px;">${input.code}</p>
          <p>Este código expira en pocos minutos. Si no solicitaste el cambio, ignora este mensaje.</p>
        </div>
      `,
      text: `Hola ${input.nombre}, tu código para restablecer la contraseña es: ${input.code}`,
    });
  }

  private async sendMail(input: {
    to: string;
    subject: string;
    html: string;
    text: string;
  }) {
    const transporter = this.getTransporter();
    try {
      const result = (await transporter.sendMail({
        from: this.getFromAddress(),
        to: input.to,
        subject: input.subject,
        html: input.html,
        text: input.text,
      })) as SMTPTransport.SentMessageInfo;
      const accepted = this.formatRecipientList(result.accepted);
      const rejected = this.formatRecipientList(result.rejected);
      this.logger.log(
        `Correo enviado a ${input.to}. messageId=${result.messageId ?? 'N/A'} accepted=${accepted || 'N/A'} rejected=${rejected || 'N/A'}`,
      );

      if (Array.isArray(result.rejected) && result.rejected.length > 0) {
        throw new Error(`Destinatarios rechazados: ${rejected || 'N/A'}`);
      }
    } catch (error) {
      const smtpError = error as {
        code?: string;
        command?: string;
        response?: string;
        responseCode?: number;
        message?: string;
      };
      this.logger.error(
        `No se pudo enviar el correo a ${input.to}. code=${smtpError.code ?? 'N/A'} command=${smtpError.command ?? 'N/A'} responseCode=${smtpError.responseCode ?? 'N/A'} message=${smtpError.message ?? 'N/A'} response=${smtpError.response ?? 'N/A'}`,
      );
      throw new InternalServerErrorException(
        'No se pudo enviar el correo. Verifica la configuracion SMTP e intenta de nuevo.',
      );
    }
  }

  private escapeHtml(value: string) {
    return value
      .replaceAll('&', '&amp;')
      .replaceAll('<', '&lt;')
      .replaceAll('>', '&gt;')
      .replaceAll('"', '&quot;')
      .replaceAll("'", '&#39;');
  }

  private formatRecipientList(
    recipients: SMTPTransport.SentMessageInfo['accepted'],
  ) {
    if (!Array.isArray(recipients) || recipients.length === 0) {
      return '';
    }

    return recipients
      .map((recipient) =>
        typeof recipient === 'string' ? recipient : recipient.address,
      )
      .filter((recipient): recipient is string => Boolean(recipient?.trim()))
      .join(', ');
  }
}
